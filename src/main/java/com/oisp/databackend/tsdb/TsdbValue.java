package com.oisp.databackend.tsdb;

import com.fasterxml.jackson.annotation.JsonValue;

public interface TsdbValue {

    Object get();
    void set(Object o);
    @JsonValue
    String toString();
}
