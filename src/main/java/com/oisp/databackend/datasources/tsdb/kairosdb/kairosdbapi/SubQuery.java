package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.oisp.databackend.datastructures.Aggregator;
import jdk.internal.loader.AbstractClassLoaderValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubQuery {

    public static final Long MAX_NUMBER_OF_SAMPLES = 1000L * 4;

    private List<Aggregator> aggregators;
    private String name;
    private Long limit;
    private Map<String, List<String>> tags;
    private List<GroupBy> groupBy;
    private String order;


    @JsonProperty("group_by")
    public List<GroupBy> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<GroupBy> groupBy) {
        this.groupBy = groupBy;
    }

    public SubQuery withGroupByTags(List<String> tags) {
        GroupBy gb = new GroupBy();
        gb.setName("tag");
        gb.setTags(tags);
        this.groupBy.add(gb);
        return this;
    }

    public SubQuery() {
        tags = new HashMap<String, List<String>>();
        aggregators = new ArrayList<>();
        limit = MAX_NUMBER_OF_SAMPLES;
        order = "desc";
        groupBy = new ArrayList<GroupBy>();
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

    public SubQuery withLimit(Long limit) {
        if (limit != null) {
            this.limit = limit;
        }
        return this;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
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

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public SubQuery withOrder(String order) {
        setOrder(order);
        return this;
    }
    @JsonIgnore
    public Aggregator getAggregator() {
        if (aggregators.size() != 0) {
            return aggregators.get(0);
        } else {
            return null;
        }
    }

    public void setAggregator(Aggregator aggregator) {
        if (aggregators.size() != 0) {
            this.aggregators.set(0, aggregator);
        } else {
            this.aggregators.add(aggregator);
        }
    }
}
