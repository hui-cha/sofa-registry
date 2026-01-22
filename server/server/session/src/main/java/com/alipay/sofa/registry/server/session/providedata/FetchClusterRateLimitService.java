package com.alipay.sofa.registry.server.session.providedata;

import com.alipay.sofa.registry.common.model.constants.ValueConstants;
import com.alipay.sofa.registry.common.model.metaserver.ProvideData;
import com.alipay.sofa.registry.server.session.bootstrap.SessionServerConfig;
import com.alipay.sofa.registry.server.session.providedata.FetchClusterRateLimitService.ClusterRateLimitStorage;
import com.alipay.sofa.registry.server.shared.providedata.AbstractFetchSystemPropertyService;
import com.alipay.sofa.registry.server.shared.providedata.SystemDataStorage;
import com.alipay.sofa.registry.util.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.Set;

/**
 * @author huicha
 * @date 2026/1/15
 */
public class FetchClusterRateLimitService extends AbstractFetchSystemPropertyService<ClusterRateLimitStorage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FetchClusterRateLimitService.class);

  @Autowired
  private SessionServerConfig sessionServerConfig;

  protected FetchClusterRateLimitService() {
    // 默认是不开启限流的
    super(ValueConstants.CLUSTER_RATE_LIMIT_DATA_ID,
            new ClusterRateLimitStorage(INIT_VERSION, false));
  }

  @Override
  protected int getSystemPropertyIntervalMillis() {
    return this.sessionServerConfig.getSystemPropertyIntervalMillis();
  }

  @Override
  protected boolean doProcess(ClusterRateLimitStorage oldStorage, ProvideData provideData) {
    try {
      // 收到新数据，开始处理；这个数据应当是一个 JSON，格式如下
      // {
      //   "trafficOperateLimitSwitch": true
      // }
      // 1. 首先解析出新的被拉黑的 dataInfoID 列表
      String data = ProvideData.toString(provideData);
      Set<String> newDataInfoIds = JsonUtils.read(data, new TypeReference<Map<String, String>>() {});

      // 2. 首先覆盖保存，让新的 Publisher 可以被忽略，这样可以避免新增的 Publisher 被遗漏清理
      if (!this.getStorage()
              .compareAndSet(
                      oldStorage,
                      new FetchDataInfoIDBlackListService.DataInfoIDBlacklistStorage(provideData.getVersion(), newDataInfoIds))) {
        // 覆盖失败了那么可能是有并发冲突，跳过处理
        return false;
      }

      return true;
    } catch (Throwable throwable) {
      LOGGER.error("Process cluster rate limit exception", throwable);
      return false;
    }
  }

  public boolean isTrafficOperateLimitSwitch() {
    ClusterRateLimitStorage storage = this.getStorage().get();
    if (null == storage) {
      // 读取不到数据那么就认为没有开启限流
      return false;
    }
    return storage.isTrafficOperateLimitSwitch();
  }

  protected static class ClusterRateLimitStorage extends SystemDataStorage {

    /**
     * 是否针对开关流操作开启限流
     */
    private boolean trafficOperateLimitSwitch;

    public ClusterRateLimitStorage(Long version, boolean trafficOperateLimitSwitch) {
      super(version);
      this.trafficOperateLimitSwitch = trafficOperateLimitSwitch;
    }

    public boolean isTrafficOperateLimitSwitch() {
      return trafficOperateLimitSwitch;
    }
  }

}
