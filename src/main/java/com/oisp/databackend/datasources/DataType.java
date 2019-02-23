package com.oisp.databackend.datasources;


import com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.stream.Collectors;

public class DataType {
    static public enum Types {
        Boolean,
        Number,
        String,
        ByteArray
    }

    static public Map<Types, String> typeString = ImmutableMap.of(
            Types.Boolean, "Boolean",
            Types.Number, "Number",
            Types.String, "String",
            Types.ByteArray, "ByteArray"
    );

    static public List<Types> getUncoveredDataTypes(List<Types> dataTypes) {
        Map<Types, Boolean> uncovered = new HashMap<>();
        uncovered.put(Types.Boolean, true);
        uncovered.put(Types.Number, true);
        uncovered.put(Types.String, true);
        uncovered.put(Types.ByteArray, true);

        dataTypes.forEach(type -> {
            uncovered.put(type, false);
        });
        return uncovered.entrySet()
                .stream()
                .filter(entry -> entry.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    static public String getTypeString(Types type) {
        return typeString.get(type);
    }

    static public List<String> getTypesStringList(List<Types> types) {
        return types.stream()
                .map(type -> getTypeString(type))
                .collect(Collectors.toList());
    }

    static public Types getType(String type) {
        return typeString.entrySet()
                .stream()
                .filter(entry -> type.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList()).get(0);
    }
    static public List<Types> getAllTypes() {
        return Arrays.asList(Types.values());
    }
}
