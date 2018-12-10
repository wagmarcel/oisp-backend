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

package com.oisp.databackend.tsdb.opentsdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.tsdb.TsdbAccess;
import com.oisp.databackend.tsdb.TsdbObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Repository
public class TsdbAccessOpenTsdb implements TsdbAccess {

    private static final Logger logger = LoggerFactory.getLogger(TsdbAccessOpenTsdb.class);

    public static final String CONTENT_TYPE = "application/json";

    @Autowired
    private OispConfig oispConfig;

    @Override
    public boolean put(List<TsdbObject> tsdbObjects) {
        addTypeAttributes(tsdbObjects, "value");
        ObjectMapper mapper = new ObjectMapper();
        //SimpleModule module = new SimpleModule("TsdbObjectSerializer");
        //module.addSerializer(TsdbObject.class, new TsdbObjectSerializer());
        //mapper.registerModule(module);
        String jsonObject = null;
        try {
            jsonObject = mapper.writeValueAsString(tsdbObjects);
        } catch (JsonProcessingException e) {
            logger.error("Could not create JSON object for post request: " + e);
            return false;
        }
        String request = "http://"
                + oispConfig.getBackendConfig().getTsdbProperties().getProperty(OispConfig.OISP_BACKEND_TSDB_URI)
                + ":"
                + oispConfig.getBackendConfig().getTsdbProperties().getProperty(OispConfig.OISP_BACKEND_TSDB_PORT);
        String jsonObjectWithTags = jsonObject.replaceFirst(Pattern.quote("attributes"), "tags");
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(request);
        StringEntity entity = null;
        try {
            entity = new StringEntity(jsonObjectWithTags);

            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", CONTENT_TYPE);
            httpPost.setHeader("Content-type", CONTENT_TYPE);
            CloseableHttpResponse response = client.execute(httpPost);
            logger.info("Result of request: " + response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            logger.error("Could not create JSON payload for POST request: " + e);
        }
        return true;
    }


    private void addTypeAttributes(List<TsdbObject> tsdbObjects, String attr) {
        for (TsdbObject tsdbObject: tsdbObjects) {
            tsdbObject.setAttribute("type", attr);
        }
    }
    @Override
    public boolean put(TsdbObject tsdbObject) {

        List<TsdbObject> list = new ArrayList<TsdbObject>();
        list.add(tsdbObject);

        return put(list);
    }


    @Override
    public TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop) {

        return new TsdbObject[0];
    }

    @Override
    public TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop, boolean forward, int limit) {
        return new TsdbObject[0];
    }


    public String[] scanForAttributeNames(TsdbObject tsdbObject, long start, long stop) throws IOException {

        return new String[0];
    }

}
