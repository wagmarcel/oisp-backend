package com.oisp.databackend.tsdb.opentsdb;

public class OpenTsdbDescriptor {

    private String url;
    private int port;

    public void setPort(int port) {
        this.port = port;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getPort() {
        return port;
    }

    public String getUrl() {
        return url;
    }
}
