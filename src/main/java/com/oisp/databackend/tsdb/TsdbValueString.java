package com.oisp.databackend.tsdb;


import com.fasterxml.jackson.annotation.JsonValue;

public class TsdbValueString implements TsdbValue {

    public TsdbValueString(String value) {
        this.value = value;
    }

    public TsdbValueString(){

    }

    private String value;

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
