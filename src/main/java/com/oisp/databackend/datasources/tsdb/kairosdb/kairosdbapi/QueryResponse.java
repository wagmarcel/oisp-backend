package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

import java.util.List;

public class QueryResponse {
    private List<Queries> queries;

    public List<Queries> getQueries() {
        return queries;
    }

    public void setQueries(List<Queries> queries) {
        this.queries = queries;
    }
}
