package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class Query {
    private long start_absolute;
    private long end_absolute;
    private List<SubQuery> metrics;
    //private boolean msResolution;

    public Query() {
        this.metrics = new ArrayList<SubQuery>();
      //  this.msResolution = true;
    }
    public Query withStart(long start) {
        this.start_absolute = start;
        return this;
    }

    public Query withEnd(long end) {
        this.end_absolute = end;
        return this;
    }
    public void setEnd_absolute(long end_absolute) {
        this.end_absolute = end_absolute;
    }

    public void setMetrics(List<SubQuery> metrics) {
        this.metrics = metrics;
    }

    public void setStart_absolute(long start_absolute) {
        this.start_absolute = start_absolute;
    }

    /*public void setMsResolution(boolean msResolution) {
        this.msResolution = msResolution;
    }*/

    public long getEnd_absolute() {
        return end_absolute;
    }

    public long getStart_absolute() {
        return start_absolute;
    }

    public List<SubQuery> getMetrics() {
        return metrics;
    }

    public void addQuery(SubQuery subQuery) {
        this.metrics.add(subQuery);
    }

    /*public boolean isMsResolution() {
        return msResolution;
    }*/

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String jsonObject = null;
        try {
            jsonObject = mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return null;
        }
        return jsonObject;
    }
}
