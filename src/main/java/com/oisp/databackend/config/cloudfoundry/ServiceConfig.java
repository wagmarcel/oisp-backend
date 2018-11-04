/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oisp.databackend.config.cloudfoundry;

import com.oisp.databackend.config.ServiceConfigProvider;
import com.oisp.databackend.config.cloudfoundry.utils.VcapReader;
import com.oisp.databackend.tsdb.hbase.KerberosProperties;
import com.oisp.databackend.exceptions.VcapEnvironmentException;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class ServiceConfig implements ServiceConfigProvider {

    public static final String KAFKA_SERVICE_NAME = "kafka";
    public static final String KAFKA_SERVICE_URI = "uri";
    public static final String KAFKA_UPS_NAME = "kafka-ups";
    public static final String KAFKA_UPS_TOPICS = "topics";
    public static final String KAFKA_UPS_ENABLED = "enabled";
    public static final String KAFKA_UPS_PARTITIONS = "partitions";
    public static final String KAFKA_UPS_REPLICATION = "replication";
    public static final String KAFKA_UPS_TIMEOUT_MS = "timeout_ms";
    public static final String KAFKA_OBSERVATIONS_TOPIC = "observations";
    public static final String KAFKA_HEARTBEAT = "heartbeat";
    public static final String KAFKA_HEARTBEAT_TOPIC = "name";
    public static final String KAFKA_HEARTBEAT_INTERVAL = "interval";

    public static final String ZOOKEEPER_BROKER_NAME = "zookeeper";
    public static final String ZOOKEEPER_BROKER_URI = "zk.cluster";
    public static final String ZOOKEEPER_BROKER_PLAN = "plan";

    public static final String KERBEROS_SERVICE_NAME = "kerberos";
    public static final String KRB_USER = "kuser";
    public static final String KRB_PASS = "kpassword";
    public static final String KRB_REALM = "krealm";
    public static final String KRB_KDC = "kdc";

    public static final String BACKEND_TSDB_NAME = "BACKEND_TSDB";

    public static final String LOCAL_PLAN = "local";

    @Autowired
    private VcapReader vcapReaderServices;
    private JSONObject kafkaCredentials;
    private JSONObject kafkaSettings;
    private JSONObject zookeeperService;
    private JSONObject zookeeperCredentials;
    private JSONObject kerberosCredentials;

    public ServiceConfig() {
    }

    @PostConstruct
    public void init() {
        kafkaCredentials = vcapReaderServices.getVcapServiceCredentialsByType(KAFKA_SERVICE_NAME);
        kafkaSettings = vcapReaderServices.getUserProvidedServiceCredentialsByName(KAFKA_UPS_NAME);
        zookeeperService = vcapReaderServices.getVcapServiceByType(ZOOKEEPER_BROKER_NAME);
        zookeeperCredentials = vcapReaderServices.getVcapServiceCredentialsByType(ZOOKEEPER_BROKER_NAME);
        kerberosCredentials = vcapReaderServices.getVcapServiceCredentialsByType(KERBEROS_SERVICE_NAME);
    }

    @Override
    public String getKafkaUri() throws VcapEnvironmentException {
        return getFieldValueFromJson(kafkaCredentials, KAFKA_SERVICE_NAME, KAFKA_SERVICE_URI, String.class);
    }

    @Override
    public String getZookeeperUri() throws VcapEnvironmentException {
        /*
          This is dirty workaround for dev's local machines
          On local kafka instance we cannot use '/kafka' postfix in URI
         */
        String plan = getFieldValueFromJson(zookeeperService, ZOOKEEPER_BROKER_NAME,
                ZOOKEEPER_BROKER_PLAN, String.class);
        if (StringUtils.isNotEmpty(plan) && plan.equals(LOCAL_PLAN)) {
            return getFieldValueFromJson(zookeeperCredentials, ZOOKEEPER_BROKER_NAME,
                    ZOOKEEPER_BROKER_URI, String.class);
        } else {
            return getFieldValueFromJson(zookeeperCredentials, ZOOKEEPER_BROKER_NAME,
                    ZOOKEEPER_BROKER_URI, String.class) + "/kafka";
        }
    }

    @Override
    public Boolean isKafkaEnabled() throws VcapEnvironmentException {
        return getFieldValueFromJson(kafkaSettings, KAFKA_UPS_NAME, KAFKA_UPS_ENABLED, Boolean.class);
    }

    @Override
    public String getKafkaObservationsTopicName() throws VcapEnvironmentException {
     	try {
            JSONObject kafkaTopics = kafkaSettings.getJSONObject(KAFKA_UPS_TOPICS);
            return getFieldValueFromJson(kafkaTopics, KAFKA_UPS_TOPICS, KAFKA_OBSERVATIONS_TOPIC, String.class);
        } catch (JSONException e) {
            throw new VcapEnvironmentException("Cannot get kafka observation topic name", e);
        }
    }

    @Override
    public String getKafkaHeartbeatTopicName() throws VcapEnvironmentException {
        try {
            JSONObject kafkaHeartbeat = kafkaSettings.getJSONObject(KAFKA_UPS_TOPICS).getJSONObject(KAFKA_HEARTBEAT);
            return getFieldValueFromJson(kafkaHeartbeat, KAFKA_HEARTBEAT, KAFKA_HEARTBEAT_TOPIC, String.class);
        } catch (JSONException e) {
            throw new VcapEnvironmentException("Cannot get kafka heartbeat topic name", e);
        }
    }

    @Override
    public Integer getKafkaHeartbeatInterval() throws VcapEnvironmentException {
        try {
            JSONObject kafkaHeartbeat = kafkaSettings.getJSONObject(KAFKA_UPS_TOPICS).getJSONObject(KAFKA_HEARTBEAT);
            return getFieldValueFromJson(kafkaHeartbeat, KAFKA_HEARTBEAT, KAFKA_HEARTBEAT_INTERVAL, Integer.class);
        } catch (JSONException e) {
            throw new VcapEnvironmentException("Cannot get kafka heartbeat interval", e);
        }
    }

    @Override
    public Integer getKafkaPartitionsFactor() throws VcapEnvironmentException {
        return getFieldValueFromJson(kafkaSettings, KAFKA_UPS_NAME, KAFKA_UPS_PARTITIONS, Integer.class);
    }

    @Override
    public Integer getKafkaReplicationFactor() throws VcapEnvironmentException {
        return getFieldValueFromJson(kafkaSettings, KAFKA_UPS_NAME, KAFKA_UPS_REPLICATION, Integer.class);
    }

    @Override
    public Integer getKafkaTimeoutInMs() throws VcapEnvironmentException {
        return getFieldValueFromJson(kafkaSettings, KAFKA_UPS_NAME, KAFKA_UPS_TIMEOUT_MS, Integer.class);
    }

    @Override
    public KerberosProperties getKerberosCredentials() throws VcapEnvironmentException {
        KerberosProperties kerberosProperties = null;
        if (kerberosCredentials != null) {
            kerberosProperties = new KerberosProperties();
            kerberosProperties.setKdc(getFieldValueFromJson(kerberosCredentials, KERBEROS_SERVICE_NAME, KRB_KDC, String.class));
            kerberosProperties.setPassword(getFieldValueFromJson(kerberosCredentials, KERBEROS_SERVICE_NAME, KRB_PASS, String.class));
            kerberosProperties.setUser(getFieldValueFromJson(kerberosCredentials, KERBEROS_SERVICE_NAME, KRB_USER, String.class));
            kerberosProperties.setRealm(getFieldValueFromJson(kerberosCredentials, KERBEROS_SERVICE_NAME, KRB_REALM, String.class));
        }
        return kerberosProperties;
    }

    @SuppressWarnings("unchecked")
    private <T> T getFieldValueFromJson(JSONObject jsonObj, String type, String field, Class<T> tClass)
            throws VcapEnvironmentException {
        if (jsonObj != null) {
            try {
                if (tClass.equals(String.class)) {
                    return (T) jsonObj.getString(field);
                } else if (tClass.equals(Integer.class)) {
                    return (T) (Integer) jsonObj.getInt(field);
                } else if (tClass.equals(Boolean.class)) {
                    return (T) (Boolean) jsonObj.getBoolean(field);
                } else {
                    return null;
                }
            } catch (JSONException e) {
                throw new VcapEnvironmentException("Unable to parse json config from VCAP env - " + type, e);
            }
        } else {
            throw new VcapEnvironmentException("Unable to find json config in VCAP env - " + type);
        }
    }
}
