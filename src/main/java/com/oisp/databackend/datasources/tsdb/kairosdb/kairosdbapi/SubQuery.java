package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubQuery {

    public static final String AGGREGATOR_MAX = "max";
    public static final String AGGREGATOR_MIN = "min";
    public static final String AGGREGATOR_NONE = "none";
    public static final Integer MAX_NUMBER_OF_SAMPLES = 1024;

    private List<Aggregator> aggregators;
    private String name;
    //private String downsample;
    private Integer limit;
    private Map<String, String> tags;


    public SubQuery() {
        tags = new HashMap<String, String>();
        aggregators = new ArrayList<>();
        limit = MAX_NUMBER_OF_SAMPLES;
        //downsample = null;
    }

    public SubQuery withMetric(String metric) {
        this.name = metric;
        return this;
    }

    public SubQuery withAggregator(Aggregator aggregator) {
        this.aggregators.add(aggregator);
        return this;
    }

    public SubQuery withTag(String tagK, String tagV) {
        this.tags.put(tagK, tagV);
        return this;
    }

    public SubQuery withDownsample(String downsample) {
        //this.downsample = downsample;
        return this;
    }

    public void setAggregator(Aggregator aggregator) {
        this.aggregators = Arrays.asList(aggregator);
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<Aggregator> getAggregator() {
        return aggregators;
    }

    public String getName() {
        return name;
    }

    /*public String getDownsample() {
        return downsample;
    }

    public void setDownsample(String downsample) {
        this.downsample = downsample;
    }*/
}
