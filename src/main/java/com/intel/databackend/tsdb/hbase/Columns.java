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

package com.intel.databackend.tsdb.hbase;

import org.apache.hadoop.hbase.util.Bytes;

final class Columns {

    public static final String COLUMN_FAMILY = "data";
    public static final byte[] BYTES_COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY);

    public static final String DATA_COLUMN = "measure_val";
    public static final byte[] BYTES_DATA_COLUMN = Bytes.toBytes(DATA_COLUMN);

    public static final String ATTRIBUTE_COLUMN_PREFIX = "attibute:";
    public static final byte[] BYTES_ATTRIBUTE_COLUMN_PREFIX = Bytes.toBytes(ATTRIBUTE_COLUMN_PREFIX);

    public static final short GPS_COLUMN_SIZE = 3;

    private Columns() {

    }
}
