package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.List;

public class Aggregator {
    private String name;
    private Sampling sampling;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Sampling getSampling() {
        return sampling;
    }

    public void setSampling(Sampling sampling) {
        this.sampling = sampling;
    }
}
