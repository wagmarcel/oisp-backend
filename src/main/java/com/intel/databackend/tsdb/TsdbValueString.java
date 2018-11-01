package com.intel.databackend.tsdb;

public class TsdbValueString extends TsdbValue {
    String value;
    @Override
    public String getValue() {
        return value;
    }

    @Override
    public void setValue(Object o) {
        value = (String)o;
    }
}
