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

import com.intel.databackend.datasources.hbase.*;
import com.intel.databackend.datastructures.Observation;
import com.intel.databackend.tsdb.TsdbObject;
import com.intel.databackend.tsdb.TsdbValue;
import com.intel.databackend.tsdb.TsdbAccess;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.*;

@Repository
public class tsdbAccessHBase implements TsdbAccess {

    private static final Logger logger = LoggerFactory.getLogger(tsdbAccessHBase.class);
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
    public tsdbAccessHBase(@Value("${vcap.application.name:local}") String hbasePrefix) {
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
    public boolean put(List<TsdbObject> tsdbObjects) {

        try (Table table = getHbaseTable()) {

            List<Put> puts = new ArrayList<Put>();
            for (TsdbObject obs : tsdbObjects) {
                puts.add(getPutForObservation(obs));
            }
            logger.error("Marcel: sending data with value " + tsdbObjects.get(0).value().getValue());
            table.put(puts);
        } catch (IOException ex) {
            return false;
        }
        return true;
    }

    @Override
    public boolean put(TsdbObject tsdbObjects) {

        List<TsdbObject> puts = new ArrayList<TsdbObject>();
        puts.add(tsdbObjects);
        return put(puts);
    }



    private Put getPutForObservation(TsdbObject tsdbObject) {
        Put put = new Put(tsdbObject.metric());
        put.addColumn(Columns.BYTES_COLUMN_FAMILY, Columns.BYTES_DATA_COLUMN, Bytes.toBytes((String)tsdbObject.value().getValue()));
        Map<String, String> attributes = tsdbObject.attributes();
        if (attributes != null) {
            for (String k : attributes.keySet()) {
                put.addColumn(Columns.BYTES_COLUMN_FAMILY, Bytes.toBytes(Columns.ATTRIBUTE_COLUMN_PREFIX + k),
                        Bytes.toBytes(attributes.get(k)));
            }
        }
        return put;
    }


}
