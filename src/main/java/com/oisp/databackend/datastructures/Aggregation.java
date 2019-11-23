package com.oisp.databackend.datastructures;

public class Aggregation {

    public enum Type {
        NONE("none"),
        COUNT("count"),
        AVG("avg"),
        SUM("sum"),
        MIN("min"),
        MAX("max"),
        SUMSQUARES("sumsquares");

        private String value;

        Type(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
