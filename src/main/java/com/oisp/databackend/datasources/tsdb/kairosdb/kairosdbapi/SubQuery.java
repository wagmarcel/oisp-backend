package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;


import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubQuery {

    public static final Integer MAX_NUMBER_OF_SAMPLES = 1024 * 10;

    private List<Aggregator> aggregators;
    private String name;
    //private String downsample;
    private Integer limit;
    private Map<String, List<String>> tags;
    private List<GroupBy> group_by;

    public List<GroupBy> getGroup_by() {
        return group_by;
    }

    public void setGroup_by(List<GroupBy> group_by) {
        this.group_by = group_by;
    }

    public SubQuery withGroupByTags(List<String> tags) {
        GroupBy gb = new GroupBy();
        gb.setName("tag");
        gb.setTags(tags);
        this.group_by.add(gb);
        return this;
    }

    public SubQuery() {
        tags = new HashMap<String, List<String>>();
        aggregators = new ArrayList<>();
        limit = MAX_NUMBER_OF_SAMPLES;
        group_by = new ArrayList<GroupBy>();
    }

    public SubQuery withMetric(String metric) {
        this.name = metric;
        return this;
    }

    public SubQuery withAggregator(Aggregator aggregator) {
        this.aggregators.add(aggregator);
        return this;
    }

    public SubQuery withTag(String tagK, List<String> tagV) {
        this.tags.put(tagK, tagV);
        return this;
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

    public void setTags(Map<String, List<String>> tags) {
        this.tags = tags;
    }

    public Map<String, List<String>> getTags() {
        return tags;
    }

    public List<Aggregator> getAggregators() {
        return aggregators;
    }

    public void setAggregators(List<Aggregator> aggregators) {
        this.aggregators = aggregators;
    }

    public String getName() {
        return name;
    }
}
