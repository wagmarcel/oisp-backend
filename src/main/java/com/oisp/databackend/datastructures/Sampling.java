package com.oisp.databackend.datastructures;

public class Sampling {

    private String unit;
    private Long value;

    public Sampling(Long value, String unit) {
        this.value = value;
        this.unit = unit;
    }

    public Sampling() {
        this.unit = "milliseconds";
        this.value = 1L;
    }
    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
