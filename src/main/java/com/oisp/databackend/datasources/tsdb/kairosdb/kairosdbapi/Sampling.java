package com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi;

public class Sampling {
    Long value;
    String unit;

    public Sampling() {
        value = 1L;
        unit = "milliseconds";
    }

    public Sampling(Long value, String unit) {
        this.value = value;
        this.unit = "years";
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}
