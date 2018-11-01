package com.intel.databackend.tsdb;

public class TsdbValueString implements TsdbValue {

    private String value;

    public String get() {
        return value;
    }

    public void set(Object o) {
        value = (String) o;
    }
}
