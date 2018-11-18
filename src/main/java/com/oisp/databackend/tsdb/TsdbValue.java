package com.oisp.databackend.tsdb;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Abstraction for TSDB value
 * Could be e.g. Float, Int, or String
 */
public interface TsdbValue {

    /**
     *
     * @return get actual value (e.g. Float, Int)
     */
    Object get();

    /**
     *
     * @param o value to set (e.g. Float, Int)
     */
    void set(Object o);

    /**
     *
     * @return Stringified value
     */
    @JsonValue
    String toString();
}
