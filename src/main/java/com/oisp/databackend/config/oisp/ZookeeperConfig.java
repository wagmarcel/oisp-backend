package com.oisp.databackend.config.oisp;

public class ZookeeperConfig {
    private String zkCluster;
    private String zkNode;

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
