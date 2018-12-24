package com.oisp.databackend.datasources.tsdb;


import com.fasterxml.jackson.annotation.JsonValue;

public class TsdbValueString implements TsdbValue {

    private String value;

    public TsdbValueString(String value) {
        this.value = value;
    }

    public TsdbValueString() {
    }

    public String get() {
        return value;
    }

    public void set(Object o) {
        value = (String) o;
    }

    @Override
    @JsonValue
    public String toString() {
        return value;
    }
}
