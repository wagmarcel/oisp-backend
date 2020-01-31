package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.List;

public class Result {
    private String name;
    private List<GroupBy> groupBy;
    private Map<String, List<String>> tags;
    private List<Object[]> values;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("group_by")
    public List<GroupBy> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<GroupBy> groupBy) {
        this.groupBy = groupBy;
    }

    public Map<String, List<String>> getTags() {
        return tags;
    }

    public void setTags(Map<String, List<String>> tags) {
        this.tags = tags;
    }

    public List<Object[]> getValues() {
        return values;
    }

    public void setValues(List<Object[]> values) {
        this.values = values;
    }
}
