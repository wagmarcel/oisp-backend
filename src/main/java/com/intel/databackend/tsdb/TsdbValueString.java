package com.intel.databackend.tsdb;

public class TsdbValueString extends TsdbValue {
    String value;
    @Override
    public String get() {
        return value;
    }

    @Override
    public void set(Object o) {
        value = (String)o;
    }
}
