package com.oisp.databackend.datasources.tsdb.kairosdb;

import com.oisp.databackend.datasources.DataFormatter;
import com.oisp.databackend.datasources.tsdb.TsdbQuery;
import com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi.Queries;
import com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi.QueryResponse;
import com.oisp.databackend.datasources.tsdb.kairosdb.kairosdbapi.Result;
import com.oisp.databackend.datastructures.Observation;

import java.util.*;

public final class ObservationBuilder {

    private ObservationBuilder() {

    }

    private static void addTagsFromQuery(SortedMap<Long, Observation> observationsMap, QueryResponse queryResponse) {
        for (Queries queries : queryResponse.getQueries()) {
            if (queries.getSample_size() == 0) {
                continue;
            }
            for (Result result: queries.getResults()) {
                Map<String, List<String>> types = result.getTags();
                List<Object[]> dps = result.getValues();
                for (Map.Entry<String, List<String>> type : types.entrySet()) {
                    String tagK = type.getKey();
                    if (tagK.equals(TsdbObjectBuilder.TYPE)) {
                        continue;
                    }
                    for (String tagV : type.getValue()) {
                        for (Object[] entry : dps) {
                            Long timestamp = (Long) entry[0];
                            if (observationsMap.get(timestamp) != null) {
                                observationsMap.get(timestamp).getAttributes().put(tagK, tagV);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void addGpsFromQuery(SortedMap<Long, Observation> observationsMap, QueryResponse queryResponse) {
        for (Queries queries : queryResponse.getQueries()) {

            for (Result result: queries.getResults()) {
                if (queries.getSample_size() == 0) {
                    continue;
                }
                String type = result.getTags().get(TsdbObjectBuilder.TYPE).get(0); // type is unique there cannot be different types
                if (!type.equals(DataFormatter.gpsValueToString(0))
                        && !type.equals(DataFormatter.gpsValueToString(1))
                        && !type.equals(DataFormatter.gpsValueToString(2))) {
                    continue;
                }
                List<Object[]> dps = result.getValues();
                for (Object[] value: dps) {
                    Long timestamp = (Long) value[0];
                    String final_value = value[1].toString();
                    if (observationsMap.get(timestamp) != null) {
                        observationsMap.get(timestamp).getAttributes().put(type, final_value);
                    }
                }
            }
        }
    }

    private static SortedMap<Long, Observation> createBaseObservations(QueryResponse queryResponse, TsdbQuery tsdbQuery) {

        SortedMap<Long, Observation> observationMap = new TreeMap<>();

        for (Queries queries: queryResponse.getQueries()) {
            if (queries.getSample_size() == 0) {
                continue;
            }
            for (Result result: queries.getResults()) {
                if (result.getTags().get(TsdbObjectBuilder.TYPE).stream().filter(f -> f.equals(TsdbObjectBuilder.VALUE)).findAny().orElse(null) == null) {
                continue;
                }
                String metric = result.getName();
                List<Object[]> dps = result.getValues();
                for (Object[] value: dps) {
                    Long timestamp = (Long) value[0];
                    String final_value = value[1].toString();
                    Observation observation = new Observation(DataFormatter.getAccountFromMetric(metric),
                            DataFormatter.getCidFromMetric(metric),
                            timestamp, final_value, new ArrayList<Double>(), new HashMap<String, String>());
                    observation.setDataType(tsdbQuery.getComponentType());
                    observationMap.put(timestamp, observation);
                }
            }
        }
        return observationMap;
    }

    public static Observation[] createObservationFromQueryResponses(QueryResponse queryResponse, TsdbQuery tsdbQuery) {

        //first create the base objets with the value types
        SortedMap<Long, Observation> observationMap
                = createBaseObservations(queryResponse, tsdbQuery);

        //Now add the gps tags if any
        addGpsFromQuery(observationMap, queryResponse);
        convertGpsAttributesToLoc(observationMap);

        //Now add the other tags
        addTagsFromQuery(observationMap, queryResponse);

        // collect finally objects in an array
        return observationMap.values().toArray(new Observation[0]);
    }


    private static void convertGpsAttributesToLoc(SortedMap<Long, Observation> observationMap) {
        for (Observation observation: observationMap.values()) {
            Map<String, String> attributes = observation.getAttributes();
            if (attributes.isEmpty()) {
                continue;
            }
            String[] coord = {
                    attributes.get(DataFormatter.gpsValueToString(0)),
                    attributes.get(DataFormatter.gpsValueToString(1)),
                    attributes.get(DataFormatter.gpsValueToString(2))
            };
            List<Double> loc = new ArrayList<>();
            for (int i = 0; i < DataFormatter.GPS_COLUMN_SIZE;  i++) {
                if (coord[i] != null) {
                    loc.add(Double.parseDouble(coord[i]));
                }
            }
            observation.setLoc(loc);
        }
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
}
