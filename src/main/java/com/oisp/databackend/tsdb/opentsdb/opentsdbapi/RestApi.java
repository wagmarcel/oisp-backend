package com.oisp.databackend.tsdb.opentsdb.opentsdbapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.tsdb.TsdbObject;
import com.oisp.databackend.tsdb.opentsdb.TsdbAccessOpenTsdb;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class RestApi {
    private static final Logger logger = LoggerFactory.getLogger(RestApi.class);
    public static final String CONTENT_TYPE = "application/json";

    String base_path;
    String put_path;
    String query_path;

    public RestApi(OispConfig oispConfig) {
        base_path = "http://"
                + oispConfig.getBackendConfig().getTsdbProperties().getProperty(OispConfig.OISP_BACKEND_TSDB_URI)
                + ":"
                + oispConfig.getBackendConfig().getTsdbProperties().getProperty(OispConfig.OISP_BACKEND_TSDB_PORT);
        put_path = base_path + "/api/put";
        query_path = base_path + "/api/query";
    }


    public boolean put(List<TsdbObject> tsdbObjects) {
        String jsonObject = tsdbObjectToJSON(tsdbObjects);
        String jsonObjectWithTags = jsonObject.replaceAll(Pattern.quote("\"attributes\":"), "\"tags\":");
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(put_path);
        StringEntity entity = null;
        try {
            entity = new StringEntity(jsonObjectWithTags);

            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", CONTENT_TYPE);
            httpPost.setHeader("Content-type", CONTENT_TYPE);
            CloseableHttpResponse response = client.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            logger.info("StatusCode of request: " + statusCode);
            if (statusCode != 204) {
                return false;
            }

        } catch (IOException e) {
            logger.error("Could not create JSON payload for POST request: " + e);
        }
        return true;
    }

    String tsdbObjectToJSON(List<TsdbObject> tsdbObjects) {
        ObjectMapper mapper = new ObjectMapper();
        String jsonObject = null;
        try {
            jsonObject = mapper.writeValueAsString(tsdbObjects);
        } catch (JsonProcessingException e) {
            return null;
        }
        return jsonObject;
    }

    public List<QueryResponse> query(Query query) {
        String jsonObject = query.toString();
        String jsonObjectWithTags = jsonObject.replaceAll(Pattern.quote("\"attributes\":"), "\"tags\":");
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(query_path);
        StringEntity entity = null;
        String body = null;
        try {
            entity = new StringEntity(jsonObjectWithTags);

            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", CONTENT_TYPE);
            httpPost.setHeader("Content-type", CONTENT_TYPE);
            CloseableHttpResponse response = client.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            logger.info("StatusCode of request: " + statusCode);
            if (statusCode != 200) {
                return null;
            }
            HttpEntity responseEntity = response.getEntity();

            if (responseEntity != null) {
                body = EntityUtils.toString(responseEntity);
            }
            logger.info("Body of request" + body);
        } catch (IOException e) {
            logger.error("Could not create JSON payload for POST request: " + e);
            return null;
        }
        return queryResponsefromString(body);
    }
    public List<QueryResponse> queryResponsefromString(String jsonString) {
        List<QueryResponse> obj;
        try {
            ObjectMapper mapper = new ObjectMapper();
            obj = mapper.readValue(jsonString, new TypeReference<List<QueryResponse>>() {
            });
        } catch (IOException e) {
            return null;
        }
        return obj;
    }
}
