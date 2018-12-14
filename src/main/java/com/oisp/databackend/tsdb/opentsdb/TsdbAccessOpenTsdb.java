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
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.QueryResponse;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.RestApi;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.SubQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Repository
public class TsdbAccessOpenTsdb implements TsdbAccess {

    private static final Logger logger = LoggerFactory.getLogger(TsdbAccessOpenTsdb.class);


    RestApi api;

    @PostConstruct
    public void init() throws Exception{
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

        return api.put(tsdbObjects, true);
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

        QueryResponse[] queryResponses = api.query(query);

        return createTsdbObjectFromQueryResponses(queryResponses);
    }

    TsdbObject[] createTsdbObjectFromQueryResponses(QueryResponse[] queryResponses) {

        ArrayList<TsdbObject> tsdbObjects = new ArrayList<TsdbObject>();
        for (QueryResponse queryResponse: queryResponses) {
            String metric = queryResponse.getMetric();
            Map<Long, String> dps = queryResponse.getDps();
            for (Map.Entry<Long, String> entry:  dps.entrySet()) {
                Long timestamp = entry.getKey();
                String value = entry.getValue();
                TsdbObject tsdbObject = new TsdbObject(metric, new TsdbValueString(value), timestamp);
                tsdbObjects.add(tsdbObject);
            }
        }
        TsdbObject[] tsdbObjectsArray = new TsdbObject[tsdbObjects.size()];
        return tsdbObjects.toArray(tsdbObjectsArray);
    }

    @Override
    public TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop, boolean forward, int limit) {
        return new TsdbObject[0];
    }


    public String[] scanForAttributeNames(TsdbObject tsdbObject, long start, long stop) throws IOException {
        return new String[0];
    }

}
