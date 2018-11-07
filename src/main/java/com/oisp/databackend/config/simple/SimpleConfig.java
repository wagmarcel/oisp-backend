package com.oisp.databackend.config.simple;



import com.oisp.databackend.tsdb.opentsdb.OpenTsdbDescriptor;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class SimpleConfig {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConfig.class);

    public static final String BACKEND_TSDB_NAME = "BACKEND_TSDB";
    public static final String BACKEND_TSDB_NAME_DUMMY = "dummy";
    public static final String BACKEND_TSDB_NAME_HBASE = "hbase";
    public static final String BACKEND_TSDB_NAME_OPEN_TSDB = "openTSDB";


    private SimpleConfig() { //Utility class. Do not instantiate.
    }

    public static String getTSDBConfig() {
        return System.getenv(BACKEND_TSDB_NAME);
    }

    public static OpenTsdbDescriptor getTSDBDescriptor(String tsdbType){

        ObjectMapper mapper = new ObjectMapper();
        String descriptor = System.getenv(BACKEND_TSDB_NAME + "_" + tsdbType);
        OpenTsdbDescriptor openTsdbDescriptor = null;
        try {
            openTsdbDescriptor = mapper.readValue(descriptor, OpenTsdbDescriptor.class);
        } catch (IOException e) {
            logger.warn("Could not parse JSON descriptor for TSDB backend {}: {}", tsdbType, e.getMessage());
        }
        return openTsdbDescriptor;
    }
}
