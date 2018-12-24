package com.oisp.databackend.datasources;

import com.oisp.databackend.datastructures.Observation;
import com.oisp.databackend.datasources.tsdb.TsdbObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright (c) 2015 Intel Corporation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ObservationCreator {

    public static final short GPS_COLUMN_SIZE = 3;
    private Observation observation;
    private TsdbObject tsdbObject;
    private static final Logger logger = LoggerFactory.getLogger(ObservationCreator.class);
    private final String accountId;
    private final String componentId;
    private Boolean hasGps;
    private Map<String, String> attributes;

    ObservationCreator(TsdbObject tsdbObject) {
        this.accountId = DataFormatter.getAccountFromKey(tsdbObject.getMetric().toString());
        this.componentId = DataFormatter.getComponentFromKey(tsdbObject.getMetric().toString());
        this.tsdbObject = tsdbObject;
    }

    public ObservationCreator withGps(boolean hasGps) {
        this.hasGps = hasGps;
        return this;
    }

    public ObservationCreator withAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Observation create() {
        observation = new Observation();
        addBasicInformation();
        addAdditionalInformation();
        //logger.info("========================Observation");
        return observation;
    }

    private void addBasicInformation() {
        observation.setCid(componentId);
        observation.setAid(accountId);
        observation.setOn(tsdbObject.getTimestamp()); //0L;
        observation.setValue(tsdbObject.getValue());
        observation.setAttributes(new HashMap<String, String>());
    }

    private void addAdditionalInformation() {

        if (hasGps) {
            addLocationData();
        }
        if (attributes != null) {
            addAttributesData(attributes);
        }

    }

    private void addAttributesData(Map<String, String> attributes) {
        observation.setAttributes(attributes);
    }

    private void addLocationData() {
        try {
            String[] coordinate = new String[GPS_COLUMN_SIZE];
            observation.setLoc(new ArrayList<Double>());
            for (int i = 0; i < GPS_COLUMN_SIZE; i++) {
                coordinate[i] = attributes.get(DataFormatter.gpsValueToString(i));
                if (coordinate[i] != null) {
                    observation.getLoc().add(Double.parseDouble(coordinate[i]));
                }
                attributes.remove(DataFormatter.gpsValueToString(i));
            }
        } catch (NumberFormatException e) {
            logger.warn("problem with parsing GPS coords... not a Double?");
        }
    }
}
