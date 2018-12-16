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
import java.util.*;
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
                .withAggregator(SubQuery.AGGREGATOR_NONE)
                .withMetric(tsdbObject.getMetric());

        // If other than type tag/attrbiute is requested we have to go with empty tag/attribute list (there is not "or" between tags in
        // openTSDB?)
        boolean keepTagMapEmpty = false;
        Map<String, String> attributes = tsdbObject.getAttributes();
        if (attributes.size() > 0) {
            for (String attribute : attributes.keySet()) {
                if (attribute != DataFormatter.gpsValueToString(0)
                        && attribute != DataFormatter.gpsValueToString(1)
                        && attribute != DataFormatter.gpsValueToString(2)) {
                    keepTagMapEmpty = true;
                    break;
                }
            }

        }

        if (keepTagMapEmpty) {
            tsdbObject.setAllAttributes(new HashMap<String, String>());
        } else {
            subQuery.withTag("type", "value");

            // Add GPS attributes
            //Map<String, String> attributes = tsdbObject.getAttributes();
            if (attributes.size() > 0) {
                for (String attribute : attributes.keySet()) {
                    if (attribute == DataFormatter.gpsValueToString(0)
                            || attribute == DataFormatter.gpsValueToString(1)
                            || attribute == DataFormatter.gpsValueToString(2)) {
                        String oldTag = subQuery.getTags().get("type");
                        subQuery.withTag("type", oldTag + "|" + attribute);
                    }
                }
            }
        }
        Query query = new Query().withStart(start).withEnd(stop);
        query.addQuery(subQuery);

        QueryResponse[] queryResponses = api.query(query);

        return createTsdbObjectFromQueryResponses(queryResponses);
    }

    TsdbObject[] createTsdbObjectFromQueryResponses(QueryResponse[] queryResponses) {

        SortedMap<Long, TsdbObject> tsdbObjectsMap = new TreeMap<>();

        //first create the base objets with the value types
        for (QueryResponse queryResponse: queryResponses) {
            if (!queryResponse.getTags().get("type").equals("value")) {
                continue;
            }
            String metric = queryResponse.getMetric();
            Map<Long, String> dps = queryResponse.getDps();
            for (Map.Entry<Long, String> entry:  dps.entrySet()) {
                Long timestamp = entry.getKey();
                String value = entry.getValue();
                TsdbObject tsdbObject = new TsdbObject(metric, new TsdbValueString(value), timestamp);
                tsdbObjectsMap.put(timestamp, tsdbObject);
            }
        }   
        //Now add the gps tags if any
        for (QueryResponse queryResponse: queryResponses) {

            String type = queryResponse.getTags().get("type");
            if (!type.equals(DataFormatter.gpsValueToString(0))
                    && !type.equals(DataFormatter.gpsValueToString(1))
                    && !type.equals(DataFormatter.gpsValueToString(2))) {
                continue;
            }
            String metric = queryResponse.getMetric();
            Map<Long, String> dps = queryResponse.getDps();
            for (Map.Entry<Long, String> entry : dps.entrySet()) {
                Long timestamp = entry.getKey();
                String value = entry.getValue();
                if (tsdbObjectsMap.get(timestamp) != null) {
                    tsdbObjectsMap.get(timestamp).setAttribute(type, value);
                }
            }
        }

        //Now add the other tags
        for (QueryResponse queryResponse: queryResponses) {
            String metric = queryResponse.getMetric();
            Map<String, String> types = queryResponse.getTags();
            Map<Long, String> dps = queryResponse.getDps();
            for (Map.Entry<String, String> type : types.entrySet()) {
                String tagK = type.getKey();
                if (tagK.equals("type")) {
                    continue;
                }
                String tagV = type.getValue();

                for (Map.Entry<Long, String> entry : dps.entrySet()) {
                    Long timestamp = entry.getKey();
                    String value = entry.getValue();
                    if (tsdbObjectsMap.get(timestamp) != null) {
                        tsdbObjectsMap.get(timestamp).setAttribute(tagK, tagV);
                    }
                }
            }
        }
        // collect finally objects in an array
        TsdbObject[] tsdbObject = tsdbObjectsMap.values().toArray(new TsdbObject[0]);
        return tsdbObject;
    }

    @Override
    public TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop, boolean forward, int limit) {
        return new TsdbObject[0];
    }


    public String[] scanForAttributeNames(TsdbObject tsdbObject, long start, long stop) throws IOException {
        return new String[]{("*")};
    }

}
