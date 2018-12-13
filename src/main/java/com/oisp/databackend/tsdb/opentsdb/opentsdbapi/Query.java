package com.oisp.databackend.tsdb.opentsdb.opentsdbapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class Query {
    private long start;
    private long end;
    private List<SubQuery> queries;

    public Query() {
        this.queries = new ArrayList<SubQuery>();
    }
    public Query withStart(long start) {
        this.start = start;
        return this;
    }

    public Query withEnd(long end) {
        this.end = end;
        return this;
    }
    public void setEnd(long end) {
        this.end = end;
    }

    public void setQueries(List<SubQuery> queries) {
        this.queries = queries;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public long getStart() {
        return start;
    }

    public List<SubQuery> getQueries() {
        return queries;
    }

    public void addQuery(SubQuery subQuery) {
        this.queries.add(subQuery);
    }

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
