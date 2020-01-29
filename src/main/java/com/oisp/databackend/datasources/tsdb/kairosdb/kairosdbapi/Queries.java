package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Queries {

    private Integer sample_size;
    private List<Result> results;

    public Integer getSample_size() {
        return sample_size;
    }

    public void setSample_size(Integer sample_size) {
        this.sample_size = sample_size;
    }

    public List<Result> getResults() {
        return results;
    }

    public void setResults(List<Result> results) {
        this.results = results;
    }
}
