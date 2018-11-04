package com.intel.databackend.tsdb;

import java.util.HashMap;
import java.util.Map;

public class TsdbObject {
    private String metric;
    private TsdbValue value;
    private long timestamp;
    private Map<String, String> attributes;

    public TsdbObject(String metric, TsdbValue value, long timestamp, Map<String, String> attributes){
        this.metric = metric;
        this.value  = value;
        this.timestamp = timestamp;
        this.attributes = attributes;
    }

    public TsdbObject(){
        attributes = new HashMap<String, String>();
    }

    public TsdbObject withMetric(String metric){ this.setMetric(metric); return this;}

    public void setMetric(String metric){this.metric = metric;}
    public void setValue(TsdbValue value){this.value = value;}
    public void setTimestamp(long timestamp){this.timestamp = timestamp;}
    public void setAttribute( String attribute_k, String attribute_v) {attributes.put(attribute_k, attribute_v);}
    public void setAllAttributes(Map<String, String> attributes) {this.attributes = attributes;}

    public String metric(){return metric;}
    public TsdbValue value(){return value;}
    public long timestamp(){return timestamp;}
    public Map<String, String> attributes(){return attributes;}
}
