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

import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.datasources.DataFormatter;
import com.oisp.databackend.tsdb.TsdbAccess;
import com.oisp.databackend.tsdb.TsdbObject;
import com.oisp.databackend.tsdb.TsdbValue;
import com.oisp.databackend.tsdb.TsdbValueString;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.Query;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.RestApi;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.SubQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Repository
public class TsdbAccessOpenTsdb implements TsdbAccess {

    private static final Logger logger = LoggerFactory.getLogger(TsdbAccessOpenTsdb.class);


    RestApi api;

    @PostConstruct
    public void init() {
        api = new RestApi(oispConfig);
    }

    @Autowired
    private OispConfig oispConfig;

    @Override
    public boolean put(List<TsdbObject> tsdbObjects) {

        List<TsdbObject> locationObjects = extractLocationObjects(tsdbObjects);
        addTypeAttributes(tsdbObjects, "value");
        if (locationObjects.size() > 0) {
            tsdbObjects.addAll(locationObjects);
        }

        return api.put(tsdbObjects);
    }

    List<TsdbObject> extractLocationObjects(List<TsdbObject> tsdbObjects) {

        List<TsdbObject> locationObjects = new ArrayList<TsdbObject>();
        tsdbObjects.forEach( (element) -> {
            Map<String, String> attributes = element.getAttributes();
            if (attributes.size() == 0) {
                return;
            }
            for(int i = 0; i < 3; i++) {
                String locationName = DataFormatter.gpsValueToString(i);
                if (attributes.get(locationName) != null){
                    TsdbObject locationObject = new TsdbObject(element);
                    TsdbValue value = new TsdbValueString(
                            element.getAttributes().get(locationName)
                    );
                    locationObject.setValue(value);
                    addTypeAttribute(locationObject, locationName);
                    attributes.remove(locationName);
                    locationObjects.add(locationObject);
                }
            }
        });
        return locationObjects;
    }



/*    void getRequest(String request, String jsonObject) {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(request);
        try {
            httpGet.setHeader("Accept", CONTENT_TYPE);
            httpGet.setHeader("Content-type", CONTENT_TYPE);
            CloseableHttpResponse response = client.execute(httpGet);
            logger.info("Result of get:" + response.getStatusLine().getStatusCode());
        } catch (IOException e) {
            logger.error("Could not send GET request: " + e);
        }
    }*/

    private void addTypeAttributes(List<TsdbObject> tsdbObjects, String attr) {
        for (TsdbObject tsdbObject: tsdbObjects) {
            tsdbObject.setAttribute("type", attr);
        }
    }

    private void addTypeAttribute(TsdbObject tsdbObject, String attr) {
        List<TsdbObject> list = new ArrayList<TsdbObject>();
        list.add(tsdbObject);
        addTypeAttributes(list, attr);
    }

    @Override
    public boolean put(TsdbObject tsdbObject) {

        List<TsdbObject> list = new ArrayList<TsdbObject>();
        list.add(tsdbObject);

        return put(list);
    }


    @Override
    public TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop) {
        SubQuery subQuery = new SubQuery()
                .withAggregator(SubQuery.AGGREGATOR_MAX)
                .withMetric(tsdbObject.getMetric())
                .withTag("type", "value");

        Query query = new Query().withStart(start).withEnd(stop);
        query.addQuery(subQuery);
/*
        ObjectMapper mapper = new ObjectMapper();
        String jsonObject = null;
        try {
            jsonObject = mapper.writeValueAsString(query);
        } catch (JsonProcessingException e) {
            logger.error("Could not create JSON object for post request: " + e);
            return null;
        }
*/

        api.query(query);
        return null;

    }

    @Override
    public TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop, boolean forward, int limit) {
        return new TsdbObject[0];
    }


    public String[] scanForAttributeNames(TsdbObject tsdbObject, long start, long stop) throws IOException {

        return new String[0];
    }

}
