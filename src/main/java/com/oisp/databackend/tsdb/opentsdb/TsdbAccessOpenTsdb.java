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
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.Query;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.QueryResponse;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.RestApi;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.SubQuery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

@Repository
public class TsdbAccessOpenTsdb implements TsdbAccess {

    static final String VALUE = "value";
    static final String TYPE = "type";
    static final int GPS_COORDINATES = 3;
    private RestApi api;
    @Autowired
    private OispConfig oispConfig;


    @PostConstruct
    public void init() throws URISyntaxException {
        //Make sure that this bean is only initiated when needed
        if (!oispConfig.getBackendConfig()
                .getTsdbName()
                .equals(oispConfig.OISP_BACKEND_TSDB_NAME_OPENTSDB)) {
            return;
        }
        api = new RestApi(oispConfig);
    }

    @Override
    public boolean put(List<TsdbObject> tsdbObjects) {

        List<TsdbObject> locationObjects = TsdbObjectBuilder.extractLocationObjects(tsdbObjects);
        TsdbObjectBuilder.addTypeAttributes(tsdbObjects, VALUE);
        if (!locationObjects.isEmpty()) {
            tsdbObjects.addAll(locationObjects);
        }

        return api.put(tsdbObjects, true);
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

        Map<String, String> attributes = tsdbObject.getAttributes();
        // If other than type tag/attrbiute is requested we have to go with empty tag/attribute list (there is not "or" between tags in
        // openTSDB?)
        boolean keepTagMapEmpty = TsdbObjectBuilder.checkforTagMap(attributes);

        if (keepTagMapEmpty) {
            tsdbObject.setAllAttributes(new HashMap<String, String>());
        } else {
            subQuery.withTag(TYPE, VALUE);

            // Add GPS attributes
            //Map<String, String> attributes = tsdbObject.getAttributes();
            if (!attributes.isEmpty()) {
                for (String attribute : attributes.keySet()) {
                    if (attribute == DataFormatter.gpsValueToString(0)
                            || attribute == DataFormatter.gpsValueToString(1)
                            || attribute == DataFormatter.gpsValueToString(2)) {
                        String oldTag = subQuery.getTags().get(TYPE);
                        subQuery.withTag(TYPE, oldTag + "|" + attribute);
                    }
                }
            }
        }
        Query query = new Query().withStart(start).withEnd(stop);
        query.addQuery(subQuery);

        QueryResponse[] queryResponses = api.query(query);
        if (queryResponses == null) {
            return null;
        }
        return TsdbObjectBuilder.createTsdbObjectFromQueryResponses(queryResponses);
    }



    @Override
    public TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop, boolean forward, int limit) {
        return new TsdbObject[0];
    }


    public String[] scanForAttributeNames(TsdbObject tsdbObject, long start, long stop) throws IOException {
        return new String[]{"*"};
    }

}
