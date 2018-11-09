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

package com.oisp.databackend.config;

import com.oisp.databackend.tsdb.hbase.KerberosProperties;
import com.oisp.databackend.exceptions.ConfigEnvironmentException;

public interface ServiceConfigProvider {

    String getKafkaUri() throws ConfigEnvironmentException;

    String getZookeeperUri() throws ConfigEnvironmentException;

    Boolean isKafkaEnabled() throws ConfigEnvironmentException;

    String getKafkaObservationsTopicName() throws ConfigEnvironmentException;

    String getKafkaHeartbeatTopicName() throws ConfigEnvironmentException;

    Integer getKafkaHeartbeatInterval() throws ConfigEnvironmentException;

    Integer getKafkaPartitionsFactor() throws ConfigEnvironmentException;

    Integer getKafkaReplicationFactor() throws ConfigEnvironmentException;

    Integer getKafkaTimeoutInMs() throws ConfigEnvironmentException;

    KerberosProperties getKerberosCredentials() throws ConfigEnvironmentException;

}
