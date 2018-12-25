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

package com.oisp.databackend.datasources.tsdb.hbase;

import com.oisp.databackend.config.oisp.TsdbHBaseCondition;
import com.oisp.databackend.datastructures.Observation;
import com.oisp.databackend.datasources.tsdb.TsdbObject;
import com.oisp.databackend.datasources.tsdb.TsdbAccess;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.*;

@Primary
@Repository
@Conditional(TsdbHBaseCondition.class)
public class TsdbAccessHBase implements TsdbAccess {
    private static final Logger logger = LoggerFactory.getLogger(TsdbAccessHBase.class);
    private final String tableName;
    private final byte[] tableNameBytes;
    private static final String DEVICE_MEASUREMENT = "_DEVICE_MEASUREMENT";

    private Connection connection;
    @Autowired
    private HbaseConnManger hbaseConnManger;

    private Connection getHbaseConnection() throws IOException {
        try {
            if (connection == null || connection.isClosed()) {
                logger.info("Creating connection");
                connection = hbaseConnManger.create();
            }
            return connection;
        } catch (LoginException e) {
            logger.error("Unable to login into hbase", e);
            throw new IOException(e);
        }
    }

    private Table getHbaseTable() throws IOException {
        return getHbaseConnection().getTable(TableName.valueOf(tableNameBytes));
    }

    @Autowired
    public TsdbAccessHBase(@Value("local") String hbasePrefix) {
        logger.info("Creating HBase. Zookeeper: ");

        this.tableName = hbasePrefix.toUpperCase() + DEVICE_MEASUREMENT;
        this.tableNameBytes = Bytes.toBytes(tableName);
    }

    @PostConstruct
    public boolean createTables() throws IOException {
        Admin admin = null;
        logger.info("Try to create {} in HBase.", tableName);
        try {
            admin = getHbaseConnection().getAdmin();
            TableManager tableManager = new TableManager(admin, TableName.valueOf(tableNameBytes));
            return tableManager.createTables();
        } catch (IOException e) {
            logger.warn("Initialization of HBase table failed.", e);
            return false;
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    @PreDestroy
    public void closeHbaseConnection() throws IOException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    public boolean put(List<Observation> observations) {

        try (Table table = getHbaseTable()) {

            List<Put> puts = new ArrayList<Put>();
            for (Observation obs : observations) {
                puts.add(getPutForObservation(obs));
            }
            table.put(puts);
        } catch (IOException ex) {
            return false;
        }
        return true;
    }

    @Override
    public boolean put(Observation observation) {

        List<Observation> puts = new ArrayList<Observation>();
        puts.add(observation);
        return put(puts);
    }


    byte[] getRowKey(Observation observation) {
        return Bytes.toBytes(observation.getAid() + "." + observation.getCid() + "." + DataFormatter.zeroPrefixedTimestamp(observation.getOn()));
    }

    byte[] getRowPrefix(Observation observation) {
        return Bytes.toBytes(observation.getAid() + "." + observation.getCid());
    }

    private Put getPutForObservation(Observation observation) {
        Put put = new Put(getRowKey(observation));
        put.addColumn(Columns.BYTES_COLUMN_FAMILY, Columns.BYTES_DATA_COLUMN, Bytes.toBytes((String) observation.getValue()));
        Map<String, String> attributes = observation.getAttributes();
        // In hbase, we treat gps coordinates as special columns
        // like the attributes. So we add all gps coordinates to attributes
        if (observation.getLoc() != null) {
            if (attributes == null) { // no attributes?, but we need the structure for location data as well
                attributes = new HashMap<String, String>();
                observation.setAttributes(attributes);
            }
            for (int i = 0; i < observation.getLoc().size() && i < ObservationCreator.GPS_COLUMN_SIZE; i++) {
                String gpsAttributeName = com.oisp.databackend.datasources.DataFormatter.gpsValueToString(i);
                observation.getAttributes().put(gpsAttributeName, observation.getLoc().get(i).toString());
            }
        }
        if (attributes != null) {
            for (String k : attributes.keySet()) {
                put.addColumn(Columns.BYTES_COLUMN_FAMILY, Bytes.toBytes(Columns.ATTRIBUTE_COLUMN_PREFIX + k),
                        Bytes.toBytes(attributes.get(k)));
            }
        }
        return put;
    }

    @Override
    public Observation[] scan(Observation observation, long start, long stop) {
        logger.debug("Scanning HBase: aid: {} cid: {} start: {} stop: {}", observation.getAid(), observation.getCid(), start, stop);
        Set<String> attributesSet = observation.getAttributes().keySet();
        Scan scan = new HbaseScanManager(new String(getRowPrefix(observation))).create(start, stop).askForData(attributesSet).getScan();
        return getObservations(observation, scan);
    }


    public Observation[] scan(Observation observation, long start, long stop, boolean forward, int limit) {
        logger.debug("Scanning HBase: aid: {} cid: {} start: {} stop: {} with limit: {}",
                observation.getAid(), observation.getCid(), start, stop, limit);
        HbaseScanManager scanManager = new HbaseScanManager(new String(getRowPrefix(observation)));
        Set<String> attributesSet = observation.getAttributes().keySet();
        if (forward) {
            scanManager.create(start, stop);
        } else {
            scanManager.create(stop, start).setReversed();
        }
        scanManager.askForData(attributesSet);

        logger.debug("Scanning with limit: {}", limit);
        Scan scan = scanManager.setCaching(limit)
                .setFilter(new PageFilter(limit))
                .getScan();
        return getObservations(observation, scan);
    }


    private Observation[] getObservations(Observation observationProto, Scan scan) {
        try (Table table = getHbaseTable(); ResultScanner scanner = table.getScanner(scan)) {
            List<Observation> observations = new ArrayList<>();
            for (Result result : scanner) {
                Observation observation = new ObservationCreator(observationProto.getAid(), observationProto.getCid())
                        .withAttributes(observationProto.getAttributes().keySet())
                        .create(result);
                observations.add(observation);
            }
            return observations.toArray(new Observation[observations.size()]);
        } catch (IOException ex) {
            logger.error("Unable to find observation in hbase", ex);
            return null;
        }
    }

    public String[] scanForAttributeNames(TsdbObject tsdbObject, long start, long stop) throws IOException {

        logger.debug("Scanning HBase: getMetric: {} start: {} stop: {}", tsdbObject.getMetric(), start, stop);

        Scan scan = new HbaseScanManager(tsdbObject.getMetric())
                .create(start, stop)
                .setFilter(new ColumnPrefixFilter(Columns.BYTES_ATTRIBUTE_COLUMN_PREFIX))
                .getScan();

        Set<String> attributeNames = retrieveAttributeNames(scan);
        return attributeNames.toArray(new String[attributeNames.size()]);
    }

    private Set<String> retrieveAttributeNames(Scan scan) throws IOException {
        Set<String> attributes = new HashSet<>();
        try (Table table = getHbaseTable(); ResultScanner scanner = table.getScanner(scan)) {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String attrName = DataFormatter.getAttrNameFromCell(cell);
                    attributes.add(attrName);
                }
            }
        }
        return attributes;
    }

    private void mergeObservationAttributesWithLoc(Observation o) {

    }

}
