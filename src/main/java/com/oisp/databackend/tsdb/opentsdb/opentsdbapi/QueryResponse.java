package com.oisp.databackend.tsdb.opentsdb.opentsdbapi;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public class QueryResponse {

    private String metric;
    private Map<String, String> tags;
    private List<String> aggregatedTags;
    private Map<Long, String> dps;

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setAggregatedTags(List<String> aggregatedTags) {
        this.aggregatedTags = aggregatedTags;
    }

    public void setDps(Map<Long, String> dps) {
        this.dps = dps;
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public List<String> getAggregatedTags() {
        return aggregatedTags;
    }

    public Map<Long, String> getDps() {
        return dps;
    }


}
