package com.intel.databackend.config.simple;


public final class SimpleConfig {
    public static final String BACKEND_TSDB_NAME = "BACKEND_TSDB";
    public static final String BACKEND_TSDB_NAME_DUMMY = "dummy";
    private SimpleConfig() { //Utility class. Do not instantiate.
    }

    public static String getTSDBConfig() {
        return System.getenv(BACKEND_TSDB_NAME);
    }
}
