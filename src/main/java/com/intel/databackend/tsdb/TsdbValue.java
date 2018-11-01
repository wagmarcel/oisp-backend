package com.intel.databackend.tsdb;

public abstract class TsdbValue {

    abstract public Object getValue();
    abstract public void setValue(Object o);
}
