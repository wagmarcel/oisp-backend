package com.oisp.databackend.datasources.tsdb.kairosdb;

import com.oisp.databackend.datasources.DataFormatter;
import com.oisp.databackend.datasources.DataType;
import com.oisp.databackend.datastructures.Observation;
import com.oisp.databackend.handlers.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class TsdbObjectBuilder {
    static final String VALUE = "value";
    static final String TYPE = "type";

    private TsdbObjectBuilder() {

    }

    private static void addTypeAttribute(TsdbObject tsdbObject, String attr) {
        tsdbObject.setAttribute(TYPE, attr);
    }

    public static void addTypeAttributes(List<TsdbObject> tsdbObjects, String attr) {
        for (TsdbObject tsdbObject: tsdbObjects) {
            addTypeAttribute(tsdbObject, attr);
        }
    }

    public static List<TsdbObject> extractLocationObjects(Observation observation) {
        if (observation.getLoc() == null || observation.getLoc().isEmpty()) {
            return new ArrayList<TsdbObject>();
        }
        String metric = DataFormatter.createMetric(observation.getAid(), observation.getCid());
        List<TsdbObject> tsdbObjects = new ArrayList<TsdbObject>();
        for (int i = 0; i < observation.getLoc().size(); i++) {
            TsdbObject tsdbObject = new TsdbObject(metric, observation.getLoc().get(i).toString(), observation.getOn());
            addTypeAttribute(tsdbObject, DataFormatter.gpsValueToString(i));
            tsdbObjects.add(tsdbObject);
        }
        return tsdbObjects;
    }

    public static List<TsdbObject> createTsdbObjectsFromObservations(List<Observation> observations, boolean onlyMetadata) {
        return observations.stream()
                .flatMap(element
                        -> getTsdbObjectsfromObservation(element, onlyMetadata).stream())
                .collect(Collectors.toList());
    }

    public static List<TsdbObject> getTsdbObjectsfromObservation(Observation o, boolean onlyMetadata) {

        List<TsdbObject> tsdbObjects = new ArrayList<TsdbObject>();
        String metric = DataFormatter.createMetric(o.getAid(), o.getCid());
        long timestamp = o.getOn();
        String value;
        if (onlyMetadata) {
            value = "1";
        } else {
            value = o.getValue();
        }
        String type;
        DataType.Types otype = DataType.getType(o.getDataType());
        if (otype == DataType.Types.Boolean) {
            type = "long";
        } else if (otype == DataType.Types.String) {
            type = "string";
        } else {
            type = "double";
        }

        TsdbObject put = new TsdbObject()
                .withMetric(metric)
                .withTimestamp(timestamp)
                .withType(type)
                .withValue(value);
        Map<String, String> attributes = o.getAttributes();
        if (attributes == null) {
            attributes = new HashMap<>();
        }
        List<TsdbObject> tsdbObjectsWithLoc = extractLocationObjects(o);
        put.setAllAttributes(attributes);
        addTypeAttribute(put, VALUE);
        tsdbObjects.add(put);
        tsdbObjects.addAll(tsdbObjectsWithLoc);
        return tsdbObjects;
    }
}
