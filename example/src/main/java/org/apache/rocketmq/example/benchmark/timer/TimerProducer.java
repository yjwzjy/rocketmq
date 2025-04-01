/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.benchmark.timer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocketMQ 定时消息生产者
 * 功能：模拟多线程定时发送消息，并统计吞吐量、延迟等性能指标
 */
public class TimerProducer {
    private static final Logger log = LoggerFactory.getLogger(TimerProducer.class);

    // 核心配置参数
    private final String topic;     // 目标Topic（通过命令行参数 -t 指定）
    private final int threadCount;  // 发送线程数（-tc）
    private final int messageSize;  // 单条消息大小（字节，-ms）

    // 时间槽 (slotsTotal 和 slotDis)：控制消息在时间轴上的分布密度，模拟不同时间段的压力
    private final int precisionMs;  // 定时消息时间精度（毫秒，-p）
    private final int slotsTotal;   // 总时间槽数量（-st）
    private final int msgsTotalPerSlotThread;  // 每个线程在每个时间槽发送的消息数（-mt）
    private final int slotDis;      // 时间槽之间的间隔（毫秒，-sd）

    // 线程池
    private final ScheduledExecutorService scheduledExecutor =
            new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("ProducerScheduleThread_"));  // 定时统计任务线程池
    private final ExecutorService sendThreadPool;   // 消息发送线程池

    // 性能统计组件
    private final StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();
    private final LinkedList<Long[]> snapshotList = new LinkedList<>(); // 快照队列（存储历史统计）

    private final DefaultMQProducer producer;    // RocketMQ生产者实例

    public TimerProducer(String[] args) {
        // 通过 Apache Commons CLI 解析命令行参数（例如 -n namesrvAddr -t topic）
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        final CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkTimerProducer", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        // 初始化参数（默认值兜底）
        final String namesrvAddr = commandLine.hasOption('n') ? commandLine.getOptionValue('t').trim() : "localhost:9876";
        topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        threadCount = commandLine.hasOption("tc") ? Integer.parseInt(commandLine.getOptionValue("tc")) : 16;
        messageSize = commandLine.hasOption("ms") ? Integer.parseInt(commandLine.getOptionValue("ms")) : 1024;
        precisionMs = commandLine.hasOption('p') ? Integer.parseInt(commandLine.getOptionValue("p")) : 1000;
        slotsTotal = commandLine.hasOption("st") ? Integer.parseInt(commandLine.getOptionValue("st")) : 100;
        msgsTotalPerSlotThread = commandLine.hasOption("mt") ? Integer.parseInt(commandLine.getOptionValue("mt")) : 5000;
        slotDis = commandLine.hasOption("sd") ? Integer.parseInt(commandLine.getOptionValue("sd")) : 1000;
        System.out.printf("namesrvAddr: %s, topic: %s, threadCount: %d, messageSize: %d, precisionMs: %d, slotsTotal: %d, msgsTotalPerSlotThread: %d, slotDis: %d%n",
                namesrvAddr, topic, threadCount, messageSize, precisionMs, slotsTotal, msgsTotalPerSlotThread, slotDis);

        // 创建线程池（固定线程数，队列无界）
        sendThreadPool = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryImpl("ProducerSendMessageThread_"));

        // 初始化 RocketMQ 生产者
        producer = new DefaultMQProducer("benchmark_producer");
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));
        producer.setNamesrvAddr(namesrvAddr);                       // NameServer地址（-n）
        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);  // 禁用压缩
    }

    /**
     * 启动定时统计任务
     */
    public void startScheduleTask() {
        // 每1秒记录一次性能快照（保留最近10次）
        scheduledExecutor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        // 每10秒打印一次统计结果（TPS、平均RT、失败次数）
        scheduledExecutor.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();
                    // 计算TPS：成功数差值 / 时间差
                    final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
                    // 计算平均RT：总耗时 / 成功数
                    final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

                    System.out.printf("Send TPS: %d, Max RT: %d, Average RT: %7.3f, Send Failed: %d, Response Failed: %d%n",
                            sendTps, statsBenchmark.getSendMessageMaxRT().get(), averageRT, end[2], end[4]);
                }
            }

            @Override
            public void run() {
                try {
                    this.printStats();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);
    }

    /**
     * 消息发送主逻辑
     *
     * @throws MQClientException
     */
    public void start() throws MQClientException {
        producer.start();   // 启动生产者
        System.out.printf("Start sending messages%n");
        List<Long> delayList = new ArrayList<>();
        // 计算基准时间：当前时间对齐到 precisionMs 精度 + 2分钟后（模拟定时消息）
        final long startDelayTime = System.currentTimeMillis() / precisionMs * precisionMs + 2 * 60 * 1000 + 10;

        // 生成所有消息的延迟时间戳（按时间槽分布）
        for (int slotCnt = 0; slotCnt < slotsTotal; slotCnt++) {
            for (int msgCnt = 0; msgCnt < msgsTotalPerSlotThread; msgCnt++) {
                long delayTime = startDelayTime + slotCnt * slotDis;
                delayList.add(delayTime);
            }
        }
        Collections.shuffle(delayList);         // 打乱发送顺序模拟随机请求，避免集中发送导致服务端热点。
        // DelayTime is from 2 minutes later.

        // 多线程发送消息
        for (int i = 0; i < threadCount; i++) {
            sendThreadPool.execute(() -> {
                for (int slotCnt = 0; slotCnt < slotsTotal; slotCnt++) {

                    for (int msgCnt = 0; msgCnt < msgsTotalPerSlotThread; msgCnt++) {
                        final long beginTimestamp = System.currentTimeMillis();

                        long delayTime = delayList.get(slotCnt * msgsTotalPerSlotThread + msgCnt);

                        final Message msg;
                        try {
                            msg = buildMessage(messageSize, topic);
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            return;
                        }
                        msg.putUserProperty("MY_RECORD_TIMER_DELIVER_MS", String.valueOf(delayTime));
                        // 设置定时投递属性（RocketMQ 内部根据此属性通过 Timer 模块延迟投递）
                        msg.getProperties().put(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(delayTime));

                        // 发送消息并统计结果
                        try {
                            producer.send(msg);

                            statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                            statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();

                            // 记录RT（响应时间）
                            final long currentRT = System.currentTimeMillis() - beginTimestamp;
                            statsBenchmark.getSendMessageSuccessTimeTotal().addAndGet(currentRT);

                            // 更新最大RT（原子操作）
                            long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            while (currentRT > prevMaxRT) {
                                if (statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT)) {
                                    break;
                                }
                                prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            }
                        } catch (RemotingException e) {     // 处理各类异常（网络、Broker错误等）
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                            sleep(3000);            // 失败后等待重试
                        } catch (InterruptedException e) {
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            sleep(3000);
                        } catch (MQClientException e) {
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                        } catch (MQBrokerException e) {
                            statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                            sleep(3000);
                        }
                    }

                }
            });
        }
    }

    private Options buildCommandlineOptions(Options options) {
        Option opt = new Option("n", "namesrvAddr", true, "Nameserver address, default: localhost:9876");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Send messages to which topic, default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("tc", "threadCount", true, "Thread count, default: 64");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ms", "messageSize", true, "Message Size, default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "precisionMs", true, "Precision (ms) for TimerMessage, default: 1000");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("st", "slotsTotal", true, "Send messages to how many slots, default: 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("mt", "msgsTotalPerSlotThread", true, "Messages total for each slot and each thread, default: 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("sd", "slotDis", true, "Time distance between two slots, default: 1000");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private Message buildMessage(final int messageSize, final String topic) throws UnsupportedEncodingException {
        Message msg = new Message();
        msg.setTopic(topic);

        // 生成固定长度消息体（填充字母 'a'）
        String body = StringUtils.repeat('a', messageSize);
        msg.setBody(body.getBytes(RemotingHelper.DEFAULT_CHARSET));     // 默认UTF-8编码

        return msg;
    }

    private void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws MQClientException {
        TimerProducer timerProducer = new TimerProducer(args);
        timerProducer.startScheduleTask();
        timerProducer.start();
    }


    /**
     * 性能统计内部类
     */
    public static class StatsBenchmarkProducer {
        // 原子计数器（线程安全）
        private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);      // 发送成功数

        private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);       // 发送失败数

        private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);  // 响应成功数

        private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);    // 响应失败数

        private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);   // 总耗时（用于计算平均RT）

        private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);              // 最大RT（实时更新）

        /**
         * 创建统计快照（用于计算TPS等指标）
         *
         * @return
         */
        public Long[] createSnapshot() {
            return new Long[]{
                    System.currentTimeMillis(),
                    this.sendRequestSuccessCount.get(),
                    this.sendRequestFailedCount.get(),
                    this.receiveResponseSuccessCount.get(),
                    this.receiveResponseFailedCount.get(),
                    this.sendMessageSuccessTimeTotal.get(),
            };
        }

        public AtomicLong getSendRequestSuccessCount() {
            return sendRequestSuccessCount;
        }

        public AtomicLong getSendRequestFailedCount() {
            return sendRequestFailedCount;
        }

        public AtomicLong getReceiveResponseSuccessCount() {
            return receiveResponseSuccessCount;
        }

        public AtomicLong getReceiveResponseFailedCount() {
            return receiveResponseFailedCount;
        }

        public AtomicLong getSendMessageSuccessTimeTotal() {
            return sendMessageSuccessTimeTotal;
        }

        public AtomicLong getSendMessageMaxRT() {
            return sendMessageMaxRT;
        }
    }

}
