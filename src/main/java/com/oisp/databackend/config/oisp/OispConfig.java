package com.oisp.databackend.config.oisp;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.oisp.databackend.exceptions.ConfigEnvironmentException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

@Service
public class OispConfig {
    public static final String OISP_BACKEND_TSDB_NAME_DUMMY = "dummy";

    private static final String OISP_BACKEND_CONFIG = "OISP_BACKEND_CONFIG";
    private static final String OISP_KAFKA_CONFIG = "OISP_KAFKA_CONFIG";
    private static final String OISP_ZOOKEEPER_CONFIG = "OISP_ZOOKEEPER_CONFIG";
    private static final String OISP_KERBEROS_CONFIG = "OISP_KERBEROS_CONFIG";
    private static final String OISP_LINK_PREFIX = "@@";

    private static final Map<String, String> varClass =
            ImmutableMap.of(OISP_BACKEND_CONFIG, "com.oisp.databackend.config.oisp.BackendConfig",
                            OISP_KAFKA_CONFIG, "com.oisp.databackend.config.oisp.KafkaConfig",
                            OISP_ZOOKEEPER_CONFIG, "com.oisp.databackend.config.oisp.ZookeeperConfig",
                            OISP_KERBEROS_CONFIG, "com.oisp.databackend.config.oisp.KerberosConfig");

    private BackendConfig backendConfig;
    private Map<String, Object> foundVars;


    @PostConstruct
    public void init() throws ConfigEnvironmentException {

        foundVars = new Hashtable<String, Object>();

        backendConfig = (BackendConfig) getObjectFromVar(OISP_BACKEND_CONFIG);
    }


    Object getObjectFromVar(String var) throws ConfigEnvironmentException {

        //Create generic object based on Env Variable
        if (foundVars.containsKey(var)) {
            return foundVars.get(var);
        }


        String rawConfig = System.getenv(var);
        if (rawConfig == null) {
            throw new ConfigEnvironmentException("Could not find environment variable " + var);
        }


        Class<?> classDef = null;
        try {
            classDef = Class.forName(varClass.get(var));
            //myObj = classDef.newInstance();
        } catch (ClassNotFoundException e) {
            throw new ConfigEnvironmentException("Could not instantiate class " + varClass.get(var), e);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Object myObj = null;
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(rawConfig);
            myObj = objectMapper.readValue(rawConfig, classDef);
        } catch (IOException e) {
            throw new ConfigEnvironmentException("Could not parse content of " + rawConfig, e);
        }

        //search all Env Variabe links, i.e. starting with @@
        for (Iterator<String> it = jsonNode.fieldNames(); it.hasNext();) {
            String key = it.next();
            JsonNode node = jsonNode.get(key);
            if (node.isTextual() && node.asText().startsWith(OISP_LINK_PREFIX)) {
                Object linkObject = getObjectFromVar(node.asText().split(OISP_LINK_PREFIX)[1]);
                try {
                    String methodName = "set" + key.substring(0, 1).toUpperCase() + key.substring(1);
                    Method method = classDef.getMethod(methodName, linkObject.getClass());
                    method.invoke(myObj, linkObject);
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    throw new ConfigEnvironmentException("Could not instantiate linked object " + node.asText(), e);
                }
            }
        }

        foundVars.put(var, myObj);
        return myObj;
    }

    public BackendConfig getBackendConfig() {
        return backendConfig;
    }

    public void setBackendConfig(BackendConfig backendConfig) {
        this.backendConfig = backendConfig;
    }

}
