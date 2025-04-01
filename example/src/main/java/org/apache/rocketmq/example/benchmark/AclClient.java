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

package org.apache.rocketmq.example.benchmark;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * AclClient 类是 RocketMQ 客户端用于实现访问控制列表（ACL）认证的工具类，其核心功能是通过生成 RPCHook 实例，为 RocketMQ 的 RPC 通信添加安全校验逻辑
 */
public class AclClient {

    public static final String ACL_ACCESS_KEY = "rocketmq2";

    public static final String ACL_SECRET_KEY = "12345678";

    public static RPCHook getAclRPCHook() {
        return getAclRPCHook(ACL_ACCESS_KEY, ACL_SECRET_KEY);
    }

    /**
     * AclClientRPCHook 是 RocketMQ 提供的实现类，用于在 RPC 请求前后插入安全校验逻辑
     * 例如：在发送请求前生成签名，并将签名、访问密钥等信息添加到请求头中，供服务端验证客户端身份
     *
     * @param ak
     * @param sk
     * @return
     */
    public static RPCHook getAclRPCHook(String ak, String sk) {
        // 将密钥信息封装到 SessionCredentials 对象并创建 AclClientRPCHook 实例
        return new AclClientRPCHook(new SessionCredentials(ak, sk));
    }
}
