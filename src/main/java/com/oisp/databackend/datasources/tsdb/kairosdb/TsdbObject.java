package com.oisp.databackend.datasources.tsdb.kairosdb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Main abstraction for Kairos TSDB objects
 *
 */
public class TsdbObject implements Serializable {
    private String name;
    private String value;
    private String type;
    private long timestamp;
    private Map<String, String> attributes;

    public TsdbObject(String metric, String value, long timestamp, Map<String, String> attributes) {
        this.name = metric;
        this.value  = value;
        this.timestamp = timestamp;
        this.attributes = attributes;
    }

    public TsdbObject(String metric, String value, long timestamp) {
        this.name = metric;
        this.value  = value;
        this.timestamp = timestamp;
        this.attributes = new HashMap<String, String>();
    }

    public TsdbObject() {
        attributes = new HashMap<String, String>();
    }

    public TsdbObject(TsdbObject o) {
        this.name = o.getName();
        this.value = o.getValue();
        this.timestamp = o.getTimestamp();
        this.attributes = new HashMap<String, String>();
    }

    public TsdbObject withMetric(String metric) {
        this.setName(metric);
        return this;
    }

    public TsdbObject withValue(String value) {
        this.value = value;
        return this;
    }

    public TsdbObject withType(String type) {
        this.type = type;
        return this;
    }

    public TsdbObject withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setAttribute(String attributeK, String attributeV) {
        attributes.put(attributeK, attributeV);
    }

    public void setAllAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
