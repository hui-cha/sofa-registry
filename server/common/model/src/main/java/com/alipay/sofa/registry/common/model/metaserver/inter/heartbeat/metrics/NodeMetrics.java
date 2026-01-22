package com.alipay.sofa.registry.common.model.metaserver.inter.heartbeat.metrics;

/**
 * @author huicha
 * @date 2026/1/15
 */
public class NodeMetrics {

  private final double cpuUsage;

  private final double load;

  public NodeMetrics(double cpuUsage, double load) {
    this.cpuUsage = cpuUsage;
    this.load = load;
  }

  public double getCpuUsage() {
    return cpuUsage;
  }

  public double getLoad() {
    return load;
  }

}
