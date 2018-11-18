package com.oisp.databackend.tsdb.opentsdb;

import com.oisp.databackend.datasources.DataFormatter;
import com.oisp.databackend.tsdb.TsdbObject;
import com.oisp.databackend.tsdb.TsdbValue;
import com.oisp.databackend.tsdb.TsdbValueString;
import com.oisp.databackend.tsdb.opentsdb.opentsdbapi.QueryResponse;

import java.util.*;

import static com.oisp.databackend.tsdb.opentsdb.TsdbAccessOpenTsdb.TYPE;
import static com.oisp.databackend.tsdb.opentsdb.TsdbAccessOpenTsdb.VALUE;

public final class TsdbObjectBuilder {

    private TsdbObjectBuilder() {

    }

    public static List<TsdbObject> extractLocationObjects(List<TsdbObject> tsdbObjects) {

        List<TsdbObject> locationObjects = new ArrayList<TsdbObject>();
        tsdbObjects.forEach((element)
                -> {
                Map<String, String> attributes = element.getAttributes();
                if (attributes.size() == 0) {
                    return;
                }
                for (int i = 0; i < TsdbAccessOpenTsdb.GPS_COORDINATES; i++) {
                    String locationName = DataFormatter.gpsValueToString(i);
                    if (attributes.get(locationName) != null) {
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


    public static void addTypeAttributes(List<TsdbObject> tsdbObjects, String attr) {
        for (TsdbObject tsdbObject: tsdbObjects) {
            tsdbObject.setAttribute(TYPE, attr);
        }
    }

    private static void addTypeAttribute(TsdbObject tsdbObject, String attr) {
        List<TsdbObject> list = new ArrayList<TsdbObject>();
        list.add(tsdbObject);
        addTypeAttributes(list, attr);
    }

    private static void addTagsFromQuery(SortedMap<Long, TsdbObject> tsdbObjectsMap, QueryResponse[] queryResponses) {
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
                    if (tsdbObjectsMap.get(timestamp) != null) {
                        tsdbObjectsMap.get(timestamp).setAttribute(tagK, tagV);
                    }
                }
            }
        }
    }

    private static void addGpsFromQuery(SortedMap<Long, TsdbObject> tsdbObjectsMap, QueryResponse[] queryResponses) {
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
                if (tsdbObjectsMap.get(timestamp) != null) {
                    tsdbObjectsMap.get(timestamp).setAttribute(type, value);
                }
            }
        }
    }


    private static SortedMap<Long, TsdbObject> createBaseTsdbObjects(QueryResponse[] queryResponses) {

        SortedMap<Long, TsdbObject> tsdbObjectsMap = new TreeMap<>();

        for (QueryResponse queryResponse: queryResponses) {
            if (!queryResponse.getTags().get(TYPE).equals(VALUE)) {
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

        return tsdbObjectsMap;
    }

    public static TsdbObject[] createTsdbObjectFromQueryResponses(QueryResponse[] queryResponses) {

        //first create the base objets with the value types
        SortedMap<Long, TsdbObject> tsdbObjectsMap
                = createBaseTsdbObjects(queryResponses);

        //Now add the gps tags if any
        addGpsFromQuery(tsdbObjectsMap, queryResponses);

        //Now add the other tags
        addTagsFromQuery(tsdbObjectsMap, queryResponses);

        // collect finally objects in an array
        return tsdbObjectsMap.values().toArray(new TsdbObject[0]);
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
