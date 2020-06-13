package com.oisp.databackend.datastructures;


public class Aggregator {

    public enum Type {
        NONE("raw"),
        COUNT("count"),
        AVG("avg"),
        SUM("sum"),
        MIN("min"),
        MAX("max"),
        DEV("dev");
        private String name;
        Type(String value) {
            this.name = value;
        }
    }

        private String name;
        private Sampling sampling;

        public static String getTypeAsName(Aggregator.Type type) {
            switch(type) {
                case COUNT:
                    return "count";
                case AVG:
                    return "avg";
                case SUM:
                    return "sum";
                case MIN:
                    return "min";
                case MAX:
                    return "max";
                case DEV:
                    return "dev";
                default:
                    return "raw";
            }
        }

        public Aggregator() {
            name = String.valueOf(Type.NONE);
        }
        public Aggregator(Aggregator.Type type) {
            name = String.valueOf(type);
        }

        public Aggregator withSampling(Sampling sampling) {
            this.sampling = sampling;
            return this;
        }

        public Aggregator withType(Aggregator.Type type) {
            name = getTypeAsName(type);
            return this;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Sampling getSampling() {
            return sampling;
        }

        public void setSampling(Sampling sampling) {
            this.sampling = sampling;
        }

}
