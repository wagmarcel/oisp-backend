package com.oisp.databackend.datasources.tsdb.opentsdb;

import com.oisp.databackend.datasources.DataFormatter;
import com.oisp.databackend.datasources.tsdb.TsdbObject;
import com.oisp.databackend.datasources.tsdb.opentsdb.opentsdbapi.QueryResponse;
import com.oisp.databackend.datastructures.Observation;

import java.util.*;
import java.util.stream.Collectors;

import static com.oisp.databackend.datasources.tsdb.opentsdb.TsdbAccessOpenTsdb.TYPE;
import static com.oisp.databackend.datasources.tsdb.opentsdb.TsdbAccessOpenTsdb.VALUE;

public final class ObservationBuilder {

    private ObservationBuilder() {

    }

    public static List<TsdbObject> extractLocationObjects(List<Observation> observations) {

        return observations.stream()
                .filter(element -> ! element.getLoc().isEmpty())
                .flatMap(element
                        -> {
                    String metric = DataFormatter.createMetric(element.getAid(), element.getCid());
                    List<TsdbObject> tsdbObjects = new ArrayList<TsdbObject>();
                    for (int i = 0; i < element.getLoc().size(); i++) {
                        TsdbObject tsdbObject = new TsdbObject(metric, element.getValue(), element.getOn(), element.getAttributes());
                        addTypeAttribute(tsdbObject, DataFormatter.gpsValueToString(i));
                        tsdbObjects.add(tsdbObject);
                    }
                    return tsdbObjects.stream();
                })
               .collect(Collectors.toList());
    }


    public static void addTypeAttributes(List<TsdbObject> tsdbObjects, String attr) {
        for (TsdbObject tsdbObject: tsdbObjects) {
           addTypeAttribute(tsdbObject, attr);
        }
    }

    private static void addTypeAttribute(TsdbObject tsdbObject, String attr) {
        tsdbObject.setAttribute(TYPE, attr);
    }

    private static void addTagsFromQuery(SortedMap<Long, Observation> observationsMap, QueryResponse[] queryResponses) {
        for (QueryResponse queryResponse: queryResponses) {
            Map<String, String> types = queryResponse.getTags();
            Map<Long, String> dps = queryResponse.getDps();
            for (Map.Entry<String, String> type : types.entrySet()) {
                String tagK = type.getKey();
                if (tagK.equals(TYPE)) {
                    continue;
                }
                String tagV = type.getValue();

                for (Map.Entry<Long, String> entry : dps.entrySet()) {
                    Long timestamp = entry.getKey();
                    if (observationsMap.get(timestamp) != null) {
                        observationsMap.get(timestamp).getAttributes().put(tagK, tagV);
                    }
                }
            }
        }
    }

    private static void addGpsFromQuery(SortedMap<Long, Observation> observationsMap, QueryResponse[] queryResponses) {
        for (QueryResponse queryResponse: queryResponses) {

            String type = queryResponse.getTags().get(TYPE);
            if (!type.equals(DataFormatter.gpsValueToString(0))
                    && !type.equals(DataFormatter.gpsValueToString(1))
                    && !type.equals(DataFormatter.gpsValueToString(2))) {
                continue;
            }
            Map<Long, String> dps = queryResponse.getDps();
            for (Map.Entry<Long, String> entry : dps.entrySet()) {
                Long timestamp = entry.getKey();
                String value = entry.getValue();
                if (observationsMap.get(timestamp) != null) {
                    observationsMap.get(timestamp).getAttributes().put(type, value);
                }
            }
        }
    }

    private static SortedMap<Long, Observation> createBaseObservations(QueryResponse[] queryResponses) {

        SortedMap<Long, Observation> observationMap = new TreeMap<>();

        for (QueryResponse queryResponse: queryResponses) {
            if (!queryResponse.getTags().get(TYPE).equals(VALUE)) {
                continue;
            }
            String metric = queryResponse.getMetric();
            Map<Long, String> dps = queryResponse.getDps();
            for (Map.Entry<Long, String> entry:  dps.entrySet()) {
                Long timestamp = entry.getKey();
                String value = entry.getValue();
                Observation observation = new Observation(DataFormatter.getAccountFromMetric(metric),
                        DataFormatter.getCidFromMetric(metric),
                        timestamp, value);
                observationMap.put(timestamp, observation);
            }
        }
        return observationMap;
    }

    public static Observation[] createObservationFromQueryResponses(QueryResponse[] queryResponses) {

        //first create the base objets with the value types
        SortedMap<Long, Observation> observationMap
                = createBaseObservations(queryResponses);

        //Now add the gps tags if any
        addGpsFromQuery(observationMap, queryResponses);

        //Now add the other tags
        addTagsFromQuery(observationMap, queryResponses);

        // collect finally objects in an array
        return observationMap.values().toArray(new Observation[0]);
    }

    public static boolean checkforTagMap(Map<String, String> attributes) {
        boolean keepTagMapEmpty = false;

        if (!attributes.isEmpty()) {
            for (String attribute : attributes.keySet()) {
                if (attribute != DataFormatter.gpsValueToString(0)
                        && attribute != DataFormatter.gpsValueToString(1)
                        && attribute != DataFormatter.gpsValueToString(2)) {
                    keepTagMapEmpty = true;
                    break;
                }
            }
        }

        return keepTagMapEmpty;
    }

    public static List<TsdbObject> createTsdbObjectsFromObservations(List<Observation> observations) {
        return observations.stream()
                .flatMap(element
                        -> getTsdbObjectsfromObservation(element).stream())
                .collect(Collectors.toList());
    }

    public static List<TsdbObject> getTsdbObjectsfromObservation(Observation o) {

        List<TsdbObject> tsdbObjects = new ArrayList<TsdbObject>();
        String metric = DataFormatter.createMetric(o.getAid(), o.getCid());
        long timestamp = o.getOn();
        String value = o.getValue();
        TsdbObject put = new TsdbObject()
                .withMetric(metric)
                .withTimestamp(timestamp)
                .withValue(value);
        put.setAllAttributes(o.getAttributes());
        List<Observation> observations = new ArrayList<Observation>();
        observations.add(o);
        List<TsdbObject> tsdbObjectsWithLoc = extractLocationObjects(observations);
        Map<String, String> attributes = o.getAttributes();
        put.setAllAttributes(attributes);
        tsdbObjects.add(put);
        return tsdbObjects;
    }

}
