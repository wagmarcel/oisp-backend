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

package com.intel.databackend.datastructures.responses;

import com.cedarsoftware.util.io.JsonWriter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.intel.databackend.datastructures.ComponentMeasurementTimestamps;

import java.util.List;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class FirstLastTimestampResponse {

    private String msgType = "inquiryComponentFirstAndLastResponse";
    private List<ComponentMeasurementTimestamps> componentsFirstLast;

    public String getMsgType() {
        return msgType;
    }

    public List<ComponentMeasurementTimestamps> getComponentsFirstLast() {
        return componentsFirstLast;
    }

    public void setComponentsFirstLast(List<ComponentMeasurementTimestamps> componentsFirstLast) {
        this.componentsFirstLast = componentsFirstLast;
    }

    public String toString() {
        return JsonWriter.objectToJson(this);
    }

}
