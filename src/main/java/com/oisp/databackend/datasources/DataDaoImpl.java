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
import com.oisp.databackend.exceptions.ConfigEnvironmentException;
import com.oisp.databackend.datasources.tsdb.TsdbAccess;
import com.oisp.databackend.datasources.tsdb.TsdbObject;
import com.oisp.databackend.datasources.tsdb.TsdbValueString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
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
    private ApplicationContext context;

    @Autowired
    public void selectDAOPlugin() throws ConfigEnvironmentException {
        String tsdbName = oispConfig.getBackendConfig().getTsdbName();
        if (oispConfig.OISP_BACKEND_TSDB_NAME_DUMMY.equals(tsdbName)) {
            logger.info("TSDB backend: dummy");
            this.tsdbAccess = (TsdbAccess) context.getBean("tsdbAccessDummy");
        } else if (oispConfig.OISP_BACKEND_TSDB_NAME_HBASE.equals(tsdbName)) {
            logger.info("TSDB backend: hbase");
            this.tsdbAccess = (TsdbAccess) context.getBean("tsdbAccessHBase");
        } else if (oispConfig.OISP_BACKEND_TSDB_NAME_OPENTSDB.equals(tsdbName)) {
            logger.info("TSDB backend: openTSDB");
            this.tsdbAccess = (TsdbAccess) context.getBean("tsdbAccessOpenTsdb");
        } else {
            throw new ConfigEnvironmentException("Could not find the backend with name " + tsdbName);
        }
    }

    @Override
    public boolean put(final Observation[] observations) {

        /*List<TsdbObject> puts = new ArrayList<TsdbObject>();
        for (Observation obs : observations) {
            puts.add(getPutForObservation(obs));
        }*/

        tsdbAccess.put(Arrays.asList(observations));

        return true;
    }

    @Override
    public Observation[] scan(String accountId, String componentId, long start, long stop, Boolean gps, String[] attributes) {
        logger.debug("Scanning HBase: acc: {} cid: {} start: {} stop: {} gps: {}", accountId, componentId, start, stop, gps);

        TsdbObject tsdbObject = new TsdbObject().withMetric(getMetric(accountId, componentId));
        if (attributes != null) {
            for (String attr: attributes) {
                tsdbObject.setAttribute(attr, "");
            }
        }
        if (gps) {
            tsdbObject.setAttribute(DataFormatter.gpsValueToString(0), "");
            tsdbObject.setAttribute(DataFormatter.gpsValueToString(1), "");
            tsdbObject.setAttribute(DataFormatter.gpsValueToString(2), "");
        }
        TsdbObject[] tsdbObjects = tsdbAccess.scan(tsdbObject, start, stop);

        return getObservations(tsdbObjects, gps);
    }

    @Override
    public Observation[] scan(String accountId, String componentId, long start, long stop, Boolean gps,
                              String[] attributes, boolean forward, int limit) {
        logger.debug("Scanning HBase: acc: {} cid: {} start: {} stop: {} gps: {} with limit: {}",
                accountId, componentId, start, stop, gps, limit);
        TsdbObject tsdbObject = new TsdbObject().withMetric(getMetric(accountId, componentId));
        TsdbObject[] tsdbObjects = tsdbAccess.scan(tsdbObject, start, stop, forward, limit);

        return getObservations(tsdbObjects, gps);
    }

    @Override
    public String[] scanForAttributeNames(String accountId, String componentId, long start, long stop) throws IOException {

        logger.debug("Scanning HBase: acc: {} cid: {} start: {} stop: {}", accountId, componentId, start, stop);
        TsdbObject tsdbObject = new TsdbObject().withMetric(getMetric(accountId, componentId));
        String[] attributesArray = tsdbAccess.scanForAttributeNames(tsdbObject, start, stop);

        //Remove locX, locY, locZ attributes as these will be processed only when requested by location flag
        String[] filteredAttr = Arrays.stream(attributesArray).filter((String s) ->
                !s.equals(DataFormatter.gpsValueToString(0))
                        && !s.equals(DataFormatter.gpsValueToString(1))
                        && !s.equals(DataFormatter.gpsValueToString(2))).toArray(String[]::new);
        return filteredAttr;
    }

    private Observation[] getObservations(TsdbObject[] tsdbObjects, boolean gps) {
        List<Observation> observations = new ArrayList<>();
        for (TsdbObject tsdbObject : tsdbObjects) {
            Observation observation = new ObservationCreator(tsdbObject)
                    .withAttributes(tsdbObject.getAttributes())
                    .withGps(gps)
                    .create();
            observations.add(observation);
        }
        return observations.toArray(new Observation[observations.size()]);

    }

    private String getMetric(String accountId, String componentId) {
        return accountId + "." + componentId;
    }

}
