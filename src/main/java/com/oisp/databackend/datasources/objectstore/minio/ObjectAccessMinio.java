package com.oisp.databackend.datasources.objectstore.minio;

import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.datasources.DataType;
import com.oisp.databackend.datasources.objectstore.ObjectStoreAccess;
import com.oisp.databackend.datastructures.Observation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import io.minio.errors.MinioException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import org.xmlpull.v1.XmlPullParserException;

@Repository
public class ObjectAccessMinio implements ObjectStoreAccess {

    private static final Logger logger = LoggerFactory.getLogger(ObjectAccessMinio.class);
    @Autowired
    private OispConfig oispConfig;

    private MinioManager minioManager;

    @PostConstruct
    public void init() throws URISyntaxException {
        //Make sure that this bean is only initiated when needed
        if (oispConfig.getBackendConfig().getObjectStoreName() == null
                || !oispConfig.getBackendConfig().getObjectStoreName().equals(oispConfig.OISP_BACKEND_OBJECT_STORE_MINIO)) {
            return;
        }
        //otherwise do the init here
        minioManager = new MinioManager(oispConfig);
    }

    @Override
    public boolean put(Observation observation) {
        List<Observation> list = new ArrayList<Observation>();
        list.add(observation);

        return put(list);
    }

    @Override
    public boolean put(List<Observation> observationList) {
        try {
            minioManager.connect();
            for (Observation o: observationList) {
                minioManager.putObject(o);
            }
        } catch (MinioException | NoSuchAlgorithmException | IOException | InvalidKeyException | XmlPullParserException e) {
            logger.error("Object store put exception: " + e);
            return false;
        }
        return true;
    }
    @Override
    public void get(Observation[] observations) {
        try {
            minioManager.connect();

            for (Observation observation : observations) {
                DataType.Types type = DataType.getType(observation.getDataType());
                if (type == DataType.Types.ByteArray) {
                    observation.setbValue(minioManager.getObject(observation));
                } else {
                    observation.setValue(new String(minioManager.getObject(observation)));
                }
            }
        } catch (MinioException |  NoSuchAlgorithmException | IOException | InvalidKeyException | XmlPullParserException e) {
            logger.error("Object store get exception: " + e);
        }
    }
}
