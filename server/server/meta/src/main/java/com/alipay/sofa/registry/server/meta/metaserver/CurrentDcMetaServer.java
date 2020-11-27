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
package com.alipay.sofa.registry.server.meta.metaserver;

import com.alipay.sofa.registry.common.model.Node;
import com.alipay.sofa.registry.common.model.metaserver.nodes.MetaNode;
import com.alipay.sofa.registry.common.model.metaserver.nodes.SessionNode;
import com.alipay.sofa.registry.observer.Observable;
import com.alipay.sofa.registry.server.meta.MetaServer;
import com.alipay.sofa.registry.server.meta.lease.LeaseManager;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Nov 23, 2020
 */
public interface CurrentDcMetaServer extends MetaServer, LeaseManager<Node>, Observable {

    List<SessionNode> getSessionServers();

    void updateClusterMembers(List<MetaNode> newMembers);

}
