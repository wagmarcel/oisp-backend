package com.intel.databackend.tsdb;

import java.util.Map;

public class TsdbObject {
    private byte[] metric;
    private TsdbValue value;
    private long timestamp;
    private Map<String, String> attributes;

    public TsdbObject(byte[] inMetric, TsdbValue inValue, long inTimestamp, Map<String, String> inAttributes){
        metric = inMetric;
        value  = inValue;
        timestamp = inTimestamp;
        attributes = inAttributes;
    }

    public TsdbObject(){}

    public void setMetric(byte[] inMetric){ metric = inMetric; }
    public void setValue(TsdbValue inValue){ value = inValue; }
    public void setTimestamp(long inTimestamp){ timestamp = inTimestamp; }
    public void setAttribute( String attribute_k, String attribute_v) { attributes.put(attribute_k, attribute_v);}
    public byte[] metric(){ return metric; }
    public TsdbValue value(){ return value; }
    public long timestamp(){ return timestamp; }
    public Map<String, String> attributes(){ return attributes; }
}
