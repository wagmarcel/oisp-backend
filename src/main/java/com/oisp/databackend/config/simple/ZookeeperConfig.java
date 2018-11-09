package com.oisp.databackend.config.simple;

public class ZookeeperConfig {
    String zkCluster;
    String zkNode;

    public void setZkCluster(String zkCluster) {
        this.zkCluster = zkCluster;
    }

    public void setZkNode(String zkNode) {
        this.zkNode = zkNode;
    }

    public String getZkCluster() {
        return zkCluster;
    }

    public String getZkNode() {
        return zkNode;
    }
}
