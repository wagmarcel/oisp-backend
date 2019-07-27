package com.oisp.databackend.datasources.objectstore.minio;

import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.datasources.DataType;
import com.oisp.databackend.datastructures.Observation;
import io.minio.MinioClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import io.minio.errors.MinioException;

import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;

public class MinioManager {

    private static final Logger logger = LoggerFactory.getLogger(MinioManager.class);
    private static final int cidSubLength = 23;
    private OispConfig oispConfig;
    private MinioClient minioClient;

    MinioManager(OispConfig oispConfig) {
        this.oispConfig = oispConfig;
    }

    String getBucketName(Observation o) {
        return o.getAid() + "." + o.getCid().substring(0, cidSubLength);
    }

    String getObjectName(Observation o) {
        return Long.toString(o.getOn());
    }

    void connect() throws MinioException {
        String uri = oispConfig.getBackendConfig().getObjectStoreProperties().getProperty(oispConfig.OISP_BACKEND_MINIO_URI);
        String port = oispConfig.getBackendConfig().getObjectStoreProperties().getProperty(oispConfig.OISP_BACKEND_MINIO_PORT);
        String accessKey = oispConfig.getBackendConfig().getObjectStoreProperties().getProperty(oispConfig.OISP_BACKEND_MINIO_ACCESSKEY);
        String secretKey = oispConfig.getBackendConfig().getObjectStoreProperties().getProperty(oispConfig.OISP_BACKEND_MINIO_SECRETKEY);
        Boolean useSSL = "true".equals(oispConfig.getBackendConfig().getObjectStoreProperties().getProperty(oispConfig.OISP_BACKEND_MINIO_USESSL));
        String protocol = "https://";
        if (!useSSL) {
            protocol = "http://";
        }
        minioClient = new MinioClient(protocol + uri + ":" + port, accessKey, secretKey);
    }

    void putObject(Observation o) throws IOException, NoSuchAlgorithmException, InvalidKeyException, XmlPullParserException, MinioException {

        String bucketName = getBucketName(o);
        String objectName = getObjectName(o);

        boolean isExist = minioClient.bucketExists(bucketName);
        if (!isExist) {
            try {
                minioClient.makeBucket(bucketName);
            } catch (MinioException e) {
                logger.warn("Race condition: tried to recreate an existing bucket. Ignoring.");
            }
        }
        DataType.Types type = DataType.getType(o.getDataType());
        if (type == DataType.Types.ByteArray) {
            InputStream byteInputStream = new ByteArrayInputStream(o.getbValue());
            minioClient.putObject(bucketName, objectName, byteInputStream, "application/octet-stream");
        } else {
            InputStream byteInputStream = new ByteArrayInputStream(o.getValue().getBytes("UTF-8"));
            minioClient.putObject(bucketName, objectName, byteInputStream, "text/plain");
        }
    }

    byte[] getObject(Observation o) throws IOException, NoSuchAlgorithmException, InvalidKeyException, XmlPullParserException, MinioException {
        String bucketName = getBucketName(o);
        String objectName = getObjectName(o);
        minioClient.statObject(bucketName, objectName);
        InputStream inputStream = minioClient.getObject(bucketName, objectName);
        return IOUtils.toByteArray(inputStream);
    }
}