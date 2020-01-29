package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

public class Sampling {
    Integer value;
    String unit;

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}
