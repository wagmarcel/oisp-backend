package com.oisp.databackend.tsdb;

public class TsdbValueString implements TsdbValue {

    private String value;

    @Override
    public String get() {
        return value;
    }

    @Override
    public void set(Object o) {
        value = (String) o;
    }
}
