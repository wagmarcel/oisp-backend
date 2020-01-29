package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import java.util.HashMap;
import java.util.List;

public class Result {
    private String name;
    private List<GroupBy> group_by;
    private HashMap<String,List<String>> tags;
    private List<Object[]> values;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<GroupBy> getGroup_by() {
        return group_by;
    }

    public void setGroup_by(List<GroupBy> group_by) {
        this.group_by = group_by;
    }

    public HashMap<String, List<String>> getTags() {
        return tags;
    }

    public void setTags(HashMap<String, List<String>> tags) {
        this.tags = tags;
    }

    public List<Object[]> getValues() {
        return values;
    }

    public void setValues(List<Object[]> values) {
        this.values = values;
    }
}
