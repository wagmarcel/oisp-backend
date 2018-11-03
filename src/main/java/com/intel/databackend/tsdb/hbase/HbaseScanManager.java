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

import com.intel.databackend.tsdb.hbase.DataFormatter;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;


class HbaseScanManager {

    private static final Logger logger = LoggerFactory.getLogger(HbaseScanManager.class);
    private Scan scan;
    private final String rowPrefix;

    private static final long MAX_DATA_PER_SCAN = 1000;
    private final PageFilter defaultPageFilter;

    public HbaseScanManager(String rowPrefix) {
        this.rowPrefix = rowPrefix;
        defaultPageFilter = new PageFilter(MAX_DATA_PER_SCAN);
    }

    public HbaseScanManager create(long start, long stop) {
        long fixedStop = DataFormatter.fixStopForExclusiveScan(start, stop);
        scan = new Scan(
                createRow(start),
                createRow(fixedStop)
        );

        scan.setFilter(defaultPageFilter);
        return this;
    }

    public byte[] createRow(long timestamp) {
        String sb = rowPrefix.toString() + '\0' + DataFormatter.zeroPrefixedTimestamp(timestamp);
        return Bytes.toBytes(sb);
    }

    public HbaseScanManager askForData( Set<String> attributes) {
        scan.addColumn(com.intel.databackend.tsdb.hbase.Columns.BYTES_COLUMN_FAMILY, com.intel.databackend.tsdb.hbase.Columns.BYTES_DATA_COLUMN);
        askForAdditionalInformation(attributes);
        return this;
    }

    public HbaseScanManager setFilter(Filter filter) {
        if (filter instanceof PageFilter && isPageLimitExceeded((PageFilter) filter)) {
            throw new IllegalArgumentException("Page size limit it to big, should be smaller than: "
                    + MAX_DATA_PER_SCAN);
        }

        scan.setFilter(filter);
        return this;
    }

    public HbaseScanManager setCaching(int limit) {
        scan.setCaching(limit);
        return this;
    }

    public HbaseScanManager setReversed() {
        scan.setReversed(true);
        return this;
    }

    public Scan getScan() {
        return scan;
    }

    private boolean isPageLimitExceeded(PageFilter filter) {
        return filter.getPageSize() > MAX_DATA_PER_SCAN;
    }

    private void askForAdditionalInformation(Set<String> attributes) {
        if (attributes != null) {
            askForAttributes(attributes);
        }
    }

    private void askForAttributes(Set<String> attributes) {
        for (String a : attributes) {
            scan.addColumn(com.intel.databackend.tsdb.hbase.Columns.BYTES_COLUMN_FAMILY, Bytes.toBytes(com.intel.databackend.tsdb.hbase.Columns.ATTRIBUTE_COLUMN_PREFIX + a));
        }
    }

    private void askForLocation() {
        logger.debug("Retrieving also GPS location");
        for (int i = 0; i < com.intel.databackend.tsdb.hbase.Columns.GPS_COLUMN_SIZE; i++) {
            scan.addColumn(com.intel.databackend.tsdb.hbase.Columns.BYTES_COLUMN_FAMILY, Bytes.toBytes(DataFormatter.gpsValueToString(i)));
        }
    }
}
