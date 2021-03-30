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
package com.alipay.sofa.registry.jdbc.elector;

import com.alipay.sofa.registry.jdbc.config.DefaultCommonConfig;
import com.alipay.sofa.registry.jdbc.config.MetaElectorConfig;
import com.alipay.sofa.registry.jdbc.domain.DistributeLockDomain;
import com.alipay.sofa.registry.jdbc.domain.FollowCompeteLockDomain;
import com.alipay.sofa.registry.jdbc.mapper.DistributeLockMapper;
import com.alipay.sofa.registry.log.Logger;
import com.alipay.sofa.registry.log.LoggerFactory;
import com.alipay.sofa.registry.store.api.elector.AbstractLeaderElector;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author xiaojian.xj
 * @version $Id: MetaJdbcLeaderElector.java, v 0.1 2021年03月12日 10:18 xiaojian.xj Exp $
 */
public class MetaJdbcLeaderElector extends AbstractLeaderElector {

  private static final Logger LOG =
      LoggerFactory.getLogger("META-ELECTOR", "[MetaJdbcLeaderElector]");

  private static final String lockName = "META-MASTER";

  @Autowired private DistributeLockMapper distributeLockMapper;

  @Autowired private MetaElectorConfig metaElectorConfig;

  @Autowired private DefaultCommonConfig defaultCommonConfig;

  /**
   * start elect, return current leader
   *
   * @return
   */
  @Override
  protected LeaderInfo doElect() {
    DistributeLockDomain lock =
        distributeLockMapper.queryDistLock(defaultCommonConfig.getClusterId(), lockName);

    /** compete and return leader */
    if (lock == null) {
      return competeLeader(defaultCommonConfig.getClusterId());
    }

    ElectorRole role = amILeader(lock.getOwner()) ? ElectorRole.LEADER : ElectorRole.FOLLOWER;
    if (role == ElectorRole.LEADER) {
      lock = onLeaderWorking(lock, myself());
    } else {
      lock = onFollowWorking(lock, myself());
    }

    LeaderInfo result =
        new LeaderInfo(
            lock.getGmtModified().getTime(),
            lock.getOwner(),
            lock.getGmtModified(),
            lock.getDuration());
    if (LOG.isInfoEnabled()) {
      LOG.info("meta role : {}, leaderInfo: {}", role, result);
    }
    return result;
  }

  /**
   * compete and return leader
   *
   * @param dataCenter
   * @return
   */
  private LeaderInfo competeLeader(String dataCenter) {
    DistributeLockDomain lock =
        new DistributeLockDomain(
            dataCenter, lockName, myself(), metaElectorConfig.getLockExpireDuration());
    try {
      // throw exception if insert fail
      distributeLockMapper.competeLockOnInsert(lock);
      // compete finish.
      lock = distributeLockMapper.queryDistLock(dataCenter, lockName);

      if (LOG.isInfoEnabled()) {
        LOG.info("meta: {} compete success, become leader.", myself());
      }
    } catch (Throwable t) {
      // compete leader error, query current leader
      lock = distributeLockMapper.queryDistLock(dataCenter, lockName);
      if (LOG.isInfoEnabled()) {
        LOG.info("meta: {} compete error, leader is: {}.", myself(), lock.getOwner());
      }
    }
    return new LeaderInfo(
        lock.getGmtModified().getTime(),
        lock.getOwner(),
        lock.getGmtModified(),
        lock.getDuration());
  }

  /**
   * query current leader
   *
   * @return
   */
  @Override
  protected LeaderInfo doQuery() {
    DistributeLockDomain lock =
        distributeLockMapper.queryDistLock(defaultCommonConfig.getClusterId(), lockName);
    if (lock == null) {
      return LeaderInfo.hasNoLeader;
    }

    return new LeaderInfo(
        lock.getGmtModified().getTime(),
        lock.getOwner(),
        lock.getGmtModified(),
        lock.getDuration());
  }

  private DistributeLockDomain onLeaderWorking(DistributeLockDomain lock, String myself) {

    try {
      /** as leader, do heartbeat */
      distributeLockMapper.ownerHeartbeat(lock);
      if (LOG.isInfoEnabled()) {
        LOG.info("leader heartbeat: {}", myself);
      }
      return distributeLockMapper.queryDistLock(lock.getDataCenter(), lock.getLockName());
    } catch (Throwable t) {
      LOG.error("leader:{} heartbeat error.", myself, t);
    }
    return lock;
  }

  public DistributeLockDomain onFollowWorking(DistributeLockDomain lock, String myself) {
    /** as follow, do compete if lock expire */
    if (lock.expire()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("lock expire: {}, meta elector start: {}", lock, myself);
      }
      distributeLockMapper.competeLockOnUpdate(
          new FollowCompeteLockDomain(
              lock.getDataCenter(),
              lock.getLockName(),
              lock.getOwner(),
              lock.getGmtModified(),
              myself));
      DistributeLockDomain newLock =
          distributeLockMapper.queryDistLock(lock.getDataCenter(), lock.getLockName());
      if (LOG.isInfoEnabled()) {
        LOG.info("elector finish, new lock: {}", lock);
      }
      return newLock;
    }
    return lock;
  }
  /**
   * Setter method for property <tt>distributeLockMapper</tt>.
   *
   * @param distributeLockMapper value to be assigned to property distributeLockMapper
   */
  public void setDistributeLockMapper(DistributeLockMapper distributeLockMapper) {
    this.distributeLockMapper = distributeLockMapper;
  }

  /**
   * Setter method for property <tt>metaElectorConfig</tt>.
   *
   * @param metaElectorConfig value to be assigned to property metaElectorConfig
   */
  public void setMetaElectorConfig(MetaElectorConfig metaElectorConfig) {
    this.metaElectorConfig = metaElectorConfig;
  }

  /**
   * Setter method for property <tt>defaultCommonConfig</tt>.
   *
   * @param defaultCommonConfig value to be assigned to property defaultCommonConfig
   */
  public void setDefaultCommonConfig(DefaultCommonConfig defaultCommonConfig) {
    this.defaultCommonConfig = defaultCommonConfig;
  }
}