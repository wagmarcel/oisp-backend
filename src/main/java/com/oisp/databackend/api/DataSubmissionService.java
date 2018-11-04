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

package com.oisp.databackend.api;

import com.oisp.databackend.api.kafka.KafkaService;
import com.oisp.databackend.datasources.DataDao;
import com.oisp.databackend.datastructures.Observation;
import com.oisp.databackend.datastructures.requests.DataSubmissionRequest;
import com.oisp.databackend.datastructures.responses.DataSubmissionResponse;
import com.oisp.databackend.exceptions.MissingDataSubmissionArgumentException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;

import static org.springframework.context.annotation.ScopedProxyMode.TARGET_CLASS;
import static org.springframework.web.context.WebApplicationContext.SCOPE_REQUEST;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.springframework.stereotype.Service
@Scope(value = SCOPE_REQUEST, proxyMode = TARGET_CLASS)
public class DataSubmissionService implements Service<DataSubmissionRequest, DataSubmissionResponse> {

    private String accountId;
    private DataSubmissionRequest request;

    private DataDao dataDao;
    private KafkaService kafkaService;

    private static final Logger logger = LoggerFactory.getLogger(DataSubmissionService.class);
    @Autowired
    public DataSubmissionService(DataDao dataDao, KafkaService kafkaService) {
        this.dataDao = dataDao;
        this.kafkaService = kafkaService;
    }

    @Override
    public Service withParams(String accountId, DataSubmissionRequest request) {
        this.accountId = accountId;
        this.request = request;
        return this;
    }

    @Override
    public DataSubmissionResponse invoke() throws MissingDataSubmissionArgumentException {
        logger.error("=============DataSubmissionResponse");
        if (request.getData() == null) {
            throw new MissingDataSubmissionArgumentException("Missing \"data\" field in request");
        }
        for (Observation o : request.getData()) {
            o.setAid(accountId);
            o.setSystemOn(this.request.getSystemOn());
        }

        dataDao.put(request.getData().toArray(new Observation[request.getData().size()]));

        kafkaService.send(request.getData());

        return new DataSubmissionResponse();
    }
}
