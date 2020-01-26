package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Aggregator {
    public static final String AGGREGATOR_COUNT = "count";
    public static final String AGGREGATOR_SUM = "sum";
    //public static final String AGGREGATOR_MIN = "min";
    //public static final String AGGREGATOR_NONE = "none";

    private String name;
    private Sampling sampling;
    private Boolean alignSampling;
    private Boolean alignStartTime;
    private Boolean alignEndTime;

    public Aggregator(String name) {
        this.name = name;
        sampling = null;
        alignEndTime = false;
        alignSampling = false;
        alignStartTime = false;
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

    @JsonProperty("align_sampling")
    public Boolean getAlignSampling() {
        return alignSampling;
    }

    public void setAlignSampling(Boolean alignSampling) {
        this.alignSampling = alignSampling;
    }

    @JsonProperty("align_start_time")
    public Boolean getAlignStartTime() {
        return alignStartTime;
    }

    public void setAlignStartTime(Boolean alignStartTime) {
        this.alignStartTime = alignStartTime;
    }

    @JsonProperty("align_end_time")
    public Boolean getAlignEndTime() {
        return alignEndTime;
    }

    public void setAlignEndTime(Boolean alignEndTime) {
        this.alignEndTime = alignEndTime;
    }
}
