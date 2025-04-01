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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocketMQ 定时消息消费者 功能：测量消息从计划投递时间到实际消费时间的延迟性能指标。
 */
public class TimerConsumer {
    private final String topic;

    // 周期性记录快照并输出统计结果（TPS、延迟分位数等）
    private final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("ConsumerScheduleThread_"));

    private final StatsBenchmarkConsumer statsBenchmark = new StatsBenchmarkConsumer();
    private final LinkedList<Long[]> snapshotList = new LinkedList<>();

    private final DefaultMQPushConsumer consumer;

    public TimerConsumer(String[] args) {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        final CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkTimerConsumer", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String namesrvAddr = commandLine.hasOption('n') ? commandLine.getOptionValue('t').trim() : "localhost:9876";
        topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        System.out.printf("namesrvAddr: %s, topic: %s%n", namesrvAddr, topic);

        consumer = new DefaultMQPushConsumer("benchmark_consumer");
        consumer.setInstanceName(Long.toString(System.currentTimeMillis()));
        consumer.setNamesrvAddr(namesrvAddr);
    }

    public void startScheduleTask() {
        // 每 1 秒记录当前时间戳、总消费次数和总延迟时间到 snapshotList，保留最近 10 条快照用于计算 TPS
        scheduledExecutor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        scheduledExecutor.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long consumeTps =
                            (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
                    final double avgDelayedDuration = (end[2] - begin[2]) / (double) (end[1] - begin[1]);

                    List<Long> delayedDurationList = new ArrayList<>(TimerConsumer.this.statsBenchmark.getDelayedDurationMsSet());
                    if (delayedDurationList.isEmpty()) {
                        System.out.printf("Consume TPS: %d, Avg delayedDuration: %7.3f, Max delayedDuration: 0, %n",
                                consumeTps, avgDelayedDuration);
                    } else {
                        long delayedDuration25 = delayedDurationList.get((int) (delayedDurationList.size() * 0.25));
                        long delayedDuration50 = delayedDurationList.get((int) (delayedDurationList.size() * 0.5));
                        long delayedDuration80 = delayedDurationList.get((int) (delayedDurationList.size() * 0.8));
                        long delayedDuration90 = delayedDurationList.get((int) (delayedDurationList.size() * 0.9));
                        long delayedDuration99 = delayedDurationList.get((int) (delayedDurationList.size() * 0.99));
                        long delayedDuration999 = delayedDurationList.get((int) (delayedDurationList.size() * 0.999));

                        System.out.printf("Consume TPS: %d, Avg delayedDuration: %7.3f, Max delayedDuration: %d, " +
                                        "delayDuration %%25: %d, %%50: %d; %%80: %d; %%90: %d; %%99: %d; %%99.9: %d%n",
                                consumeTps, avgDelayedDuration, delayedDurationList.get(delayedDurationList.size() - 1),
                                delayedDuration25, delayedDuration50, delayedDuration80, delayedDuration90, delayedDuration99, delayedDuration999);
                    }
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

    public void start() throws MQClientException {
        consumer.subscribe(topic, "*");

        // 消息监听并计算延迟 MessageListenerConcurrently 隐含并发，其线程数由  RocketMQ 客户端控制
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt msg = msgs.get(0);
                long now = System.currentTimeMillis();

                statsBenchmark.getReceiveMessageTotalCount().incrementAndGet();

                long deliverTimeMs = Long.parseLong(msg.getProperty("MY_RECORD_TIMER_DELIVER_MS"));
                long delayedDuration = now - deliverTimeMs;

                statsBenchmark.getDelayedDurationMsSet().add(delayedDuration);
                statsBenchmark.getDelayedDurationMsTotal().addAndGet(delayedDuration);

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Start receiving messages%n");
    }

    private Options buildCommandlineOptions(Options options) {
        Option opt = new Option("n", "namesrvAddr", true, "Nameserver address, default: localhost:9876");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Send messages to which topic, default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static void main(String[] args) throws MQClientException {
        TimerConsumer timerConsumer = new TimerConsumer(args);
        timerConsumer.startScheduleTask();
        timerConsumer.start();
    }


    /**
     * 性能统计内部类: 记录消费次数、总延迟时间及延迟分布
     */
    public static class StatsBenchmarkConsumer {
        private final AtomicLong receiveMessageTotalCount = new AtomicLong(0L);     // 总消费次数

        private final AtomicLong delayedDurationMsTotal = new AtomicLong(0L);       // 总延迟时间
        private final ConcurrentSkipListSet<Long> delayedDurationMsSet = new ConcurrentSkipListSet<>(); // 延迟分布集合（使用 ConcurrentSkipListSet 存储延迟时间，支持高并发写入和有序查询）

        public Long[] createSnapshot() {
            return new Long[]{
                    System.currentTimeMillis(),
                    this.receiveMessageTotalCount.get(),
                    this.delayedDurationMsTotal.get(),
            };
        }

        public AtomicLong getReceiveMessageTotalCount() {
            return receiveMessageTotalCount;
        }

        public AtomicLong getDelayedDurationMsTotal() {
            return delayedDurationMsTotal;
        }

        public ConcurrentSkipListSet<Long> getDelayedDurationMsSet() {
            return delayedDurationMsSet;
        }
    }

}
