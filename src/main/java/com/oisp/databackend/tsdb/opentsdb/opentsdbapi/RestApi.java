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
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class RestApi {
    private static final Logger logger = LoggerFactory.getLogger(RestApi.class);
    public static final String CONTENT_TYPE = "application/json";

    String host;
    int port;
    String scheme;
    URI putUri;
    URI queryUri;
    public static int MAX_CHUNK_SIZE = 4000;

    public RestApi(OispConfig oispConfig) throws URISyntaxException {
        scheme = "http";
        host = oispConfig.getBackendConfig().getTsdbProperties().getProperty(OispConfig.OISP_BACKEND_TSDB_URI);
        port = Integer.parseInt(oispConfig.getBackendConfig().getTsdbProperties().getProperty(OispConfig.OISP_BACKEND_TSDB_PORT));

        putUri = new URIBuilder()
                .setScheme(scheme)
                .setPath("/api/put")
                .setHost(host)
                .setPort(port)
                .build();
        queryUri = new URIBuilder()
                .setScheme(scheme)
                .setPath("/api/query")
                .setHost(host)
                .setPort(port)
                .setParameter("ms", null)
                .build();

    }

    public boolean put(List<TsdbObject> tsdbObjects, boolean sync) {
        List<String> jsonObjects = tsdbObjectToJSON(tsdbObjects);
        CloseableHttpClient client = HttpClients.createDefault();


        StringEntity entity = null;
        for(String jsonObject: jsonObjects) {
            String jsonObjectWithTags = jsonObject.replaceAll(Pattern.quote("\"attributes\":"), "\"tags\":");
            HttpPost httpPost = new HttpPost(putUri);
            
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
        }
        return true;
    }

    List<String> tsdbObjectToJSON(List<TsdbObject> tsdbObjects) {
        ObjectMapper mapper = new ObjectMapper();
        List<String> resultObjects = new ArrayList<String>();
        String remaining = tsdbObjects.stream()
                .map((obj) -> {
                    try {
                        return mapper.writeValueAsString(obj);
                    } catch (JsonProcessingException e) {
                        logger.warn("Could not convert object to JSON " + e);
                        return "";
                    }
                })
                .reduce(new String(), (collect, elem) -> {
                    if ((collect.isEmpty()) || (collect.length() + elem.length() > MAX_CHUNK_SIZE)) {
                        if (!collect.isEmpty()) resultObjects.add(collect + "]");
                        return "[" + elem;
                    } else {
                        return collect + "," + elem;
                    }
                });
        resultObjects.add(remaining + "]");
        return resultObjects;
    }

    public QueryResponse[] query(Query query) {
        String jsonObject = query.toString();
        String jsonObjectWithTags = jsonObject.replaceAll(Pattern.quote("\"attributes\":"), "\"tags\":");
        CloseableHttpClient client = HttpClients.createDefault();

        HttpPost httpPost = new HttpPost(queryUri);
        StringEntity entity = null;
        String body = null;
        try {
            entity = new StringEntity(jsonObjectWithTags);

            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", CONTENT_TYPE);
            httpPost.setHeader("Content-type", CONTENT_TYPE);
            CloseableHttpResponse response = client.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            logger.info("StatusCode of response: " + statusCode);
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

    public QueryResponse[] queryResponsefromString(String jsonString) {
        QueryResponse[] obj;
        try {
            ObjectMapper mapper = new ObjectMapper();
            obj = mapper.readValue(jsonString, QueryResponse[].class);
        } catch (IOException e) {
            return null;
        }
        return obj;
    }
}
