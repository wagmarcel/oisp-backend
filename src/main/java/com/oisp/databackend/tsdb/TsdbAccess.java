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

package com.oisp.databackend.tsdb;

import java.io.IOException;
import java.util.List;


/**
 * This is the abstraction API to attach different Time Series Databases backends to OISP
 * Currently work in progress, not stable API yet
 * @version 0.1
 */
public interface TsdbAccess {

    /**
     *
     * @param tsdbObject A single TSDB object to send the the data backend
     * @return true if successful, false otherwise
     */
    boolean put(TsdbObject tsdbObject);

    /**
     *
     * @param tsdbObjectList A list of TSDB objects to send to the data backend
     * @return true if successful, false otherwise
     */
    boolean put(List<TsdbObject> tsdbObjectList);

    /**
     *
     * @param tsdbObject Prototype of TSDB object to be retrieved. Contains metric and attributes to retrieve
     * @param start timestamp in ms for first object
     * @param stop timestamp in ms for last object
     * @return list of retrieved TSDB objects
     */
    TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop);

    /**
     * Can be used to scan a fixed number of samples from a timestamp, e.g. to show the 1000 most recent values
     * Useful for user interfaces with live data updates
     * @param tsdbObject Prototype of TSDB object to be retrieved. Contains metric and attributes to retrieve
     * @param start timestamp in ms for first object - when forward == true, timestamp for most recent possible
     *              timestamp otherwise
     * @param stop timestamp in ms for latest possible object - when forward == true, timestamp for first object
     *             otherwise
     * @param forward order of scanning, i.e. true means from old to new timestamps, false from new to old
     * @param limit max number of samples
     * @return list of retrieved TSDB objects
     */
    TsdbObject[] scan(TsdbObject tsdbObject, long start, long stop, boolean forward, int limit);

    /**
     * For advanced searches where all attributes are considered. Some backends need a list of all attributes
     * to retrieve them all.
     * @param tsdbObject get list of all attributes within a time interval
     * @param start timestamp in ms for first object
     * @param stop timestamp in ms for last object
     * @return Array of found tags
     * @throws IOException
     */
    String[] scanForAttributeNames(TsdbObject tsdbObject, long start, long stop) throws IOException;
}
