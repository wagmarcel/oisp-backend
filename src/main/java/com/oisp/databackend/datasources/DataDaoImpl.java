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

package com.oisp.databackend.datasources;


import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.datastructures.Observation;
import com.oisp.databackend.tsdb.TsdbAccess;
import com.oisp.databackend.tsdb.TsdbObject;
import com.oisp.databackend.tsdb.TsdbValueString;

import com.oisp.databackend.tsdb.dummy.TsdbAccessDummy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.*;

@Repository
@Configuration
public class DataDaoImpl implements DataDao {

    private static final Logger logger = LoggerFactory.getLogger(DataDaoImpl.class);

    private TsdbAccess tsdbAccess;

    @Autowired
    private OispConfig oispConfig;

    @Autowired
    public void selectDAOPlugin(TsdbAccess tsdbAccess) {
        String tsdbName = oispConfig.getBackendConfig().getTsdbName();
        if (oispConfig.OISP_BACKEND_TSDB_NAME_DUMMY.equals(tsdbName)) {
            logger.info("TSDB backend: dummy");
            this.tsdbAccess = new TsdbAccessDummy();
        } else {
            logger.info("TSDB backend: hbase");
            this.tsdbAccess = tsdbAccess;
        }
    }

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
                    .withAttributes(tsdbObject.getAttributes())
                    .create();
            observations.add(observation);
        }
        return observations.toArray(new Observation[observations.size()]);

    }

    private String getMetric(String accountId, String componentId) {
        return accountId + "." + componentId;
    }


    private TsdbObject getPutForObservation(Observation o) {
        TsdbObject put = new TsdbObject();
        String metric = getMetric(o.getAid(), o.getCid());
        put.setMetric(metric);
        long timestamp = o.getOn();
        put.setTimestamp(timestamp);
        TsdbValueString value = new TsdbValueString();
        value.set(o.getValue());
        put.setValue(value);
        if (o.getLoc() != null) {
            for (int i = 0; i < o.getLoc().size() && i < ObservationCreator.GPS_COLUMN_SIZE; i++) {
                String gpsAttributeName = DataFormatter.gpsValueToString(i);
                put.setAttribute(gpsAttributeName, o.getLoc().get(i).toString());
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
