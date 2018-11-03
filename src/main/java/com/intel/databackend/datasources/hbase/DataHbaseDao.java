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

package com.intel.databackend.datasources.hbase;

import com.intel.databackend.datastructures.Observation;
import com.intel.databackend.tsdb.TsdbAccess;
import com.intel.databackend.tsdb.TsdbObject;
import com.intel.databackend.tsdb.TsdbValueString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.*;

@Repository
public class DataHbaseDao implements DataDao {

    private static final Logger logger = LoggerFactory.getLogger(DataHbaseDao.class);

    @Autowired
    private TsdbAccess tsdbAccess;



    @Override
    public boolean put(final Observation[] observations) {

        List<TsdbObject> puts = new ArrayList<TsdbObject>();
        for (Observation obs : observations) {
            puts.add(getPutForObservation(obs));
        }

        tsdbAccess.put(puts);

        return true;
    }

    @Override
    public Observation[] scan(String accountId, String componentId, long start, long stop, Boolean gps, String[] attributes) {
        logger.debug("Scanning HBase: acc: {} cid: {} start: {} stop: {} gps: {}", accountId, componentId, start, stop, gps);

        TsdbObject tsdbObject = new TsdbObject().withMetric(getMetric(accountId, componentId));
        TsdbObject[] tsdbObjects = tsdbAccess.scan(tsdbObject, start, stop);

        return getObservations(tsdbObjects);
    }

    @Override
    public Observation[] scan(String accountId, String componentId, long start, long stop, Boolean gps,
                              String[] attributes, boolean forward, int limit) {
        logger.debug("Scanning HBase: acc: {} cid: {} start: {} stop: {} gps: {} with limit: {}",
                accountId, componentId, start, stop, gps, limit);
        TsdbObject tsdbObject = new TsdbObject().withMetric(getMetric(accountId, componentId));
        TsdbObject[] tsdbObjects = tsdbAccess.scan(tsdbObject, start, stop, forward, limit);

        return getObservations(tsdbObjects);
    }

    @Override
    public String[] scanForAttributeNames(String accountId, String componentId, long start, long stop) throws IOException {

        logger.debug("Scanning HBase: acc: {} cid: {} start: {} stop: {}", accountId, componentId, start, stop);
        TsdbObject tsdbObject = new TsdbObject().withMetric(getMetric(accountId, componentId));
        return tsdbAccess.scanForAttributeNames(tsdbObject, start, stop);
    }

    private Observation[] getObservations(TsdbObject[] tsdbObjects) {
        List<Observation> observations = new ArrayList<>();
        for (TsdbObject tsdbObject : tsdbObjects) {
            Observation observation = new ObservationCreator(tsdbObject)
                    .withAttributes(tsdbObject.attributes())
                    .create();
            observations.add(observation);
        }
        return observations.toArray(new Observation[observations.size()]);

    }

    private String getMetric(String accountId, String componentId){
        return accountId + "." + componentId;
    }


    private TsdbObject getPutForObservation(Observation o) {
        TsdbObject put = new TsdbObject();
        String metric = getMetric(o.getAid(), o.getCid());
        put.setMetric(metric);
        long timestamp = o.getOn();
        put.setTimestamp(timestamp);
        TsdbValueString value = new TsdbValueString();
        value.setValue(o.getValue());
        put.setValue(value);
        if (o.getLoc() != null) {
            for (int i = 0; i < o.getLoc().size() && i < Columns.GPS_COLUMN_SIZE; i++) {
                String gps_attribute_name = DataFormatter.gpsValueToString(i);
                put.setAttribute(gps_attribute_name, o.getLoc().get(i).toString());
            }
        }
        Map<String, String> attributes = o.getAttributes();
        if (attributes != null) {
            for (String k : attributes.keySet()) {
                put.setAttribute(k, attributes.get(k));
            }
        }
        return put;
    }

}
