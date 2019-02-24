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

package com.oisp.databackend.datasources.objectstore;

import com.oisp.databackend.datastructures.Observation;

import java.util.List;


/**
 * This is the abstraction API to attach different Time Series Databases backends to OISP
 * Currently work in progress, not stable API yet
 * @version 0.1
 */
public interface ObjectStoreAccess {

    /**
     *
     * @param observation A single Observation to send the the data backend
     * @return true if successful, false otherwise
     */
    boolean put(Observation observation);

    /**
     *
     * @param observationList A list of Observations to send to the data backend
     * @return true if successful, false otherwise
     */
    boolean put(List<Observation> observationList);

    /**
     *
     * @param observations List of Observation "hulls" to update with bValues
     */
    void get(Observation[] observations);

}
