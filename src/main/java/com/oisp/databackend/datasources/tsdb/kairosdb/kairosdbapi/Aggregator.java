package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.List;

public class Aggregator {
    public static final String AGGREGATOR_COUNT = "count";
    public static final String AGGREGATOR_SUM = "sum";
    //public static final String AGGREGATOR_MIN = "min";
    //public static final String AGGREGATOR_NONE = "none";

    private String name;
    private Sampling sampling;
    private Boolean align_sampling;
    private Boolean align_start_time;
    private Boolean align_end_time;

    public Aggregator(String name){
        this.name = name;
        sampling = null;
        align_end_time = false;
        align_sampling = false;
        align_start_time = false;
    }

    public Aggregator withSampling(Sampling sampling) {
        this.sampling = sampling;
        return this;
    }

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
