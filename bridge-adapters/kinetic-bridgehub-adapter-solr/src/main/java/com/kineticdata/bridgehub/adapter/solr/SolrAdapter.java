package com.kineticdata.bridgehub.adapter.solr;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.LoggerFactory;

public class SolrAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/

    /** Defines the adapter display name */
    public static final String NAME = "Solr Bridge";
    public static final String JSON_ROOT_DEFAULT = "$.response.docs";

    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(SolrAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(SolrAdapter.class.getResourceAsStream("/"+SolrAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+SolrAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    private String username;
    private String password;
    private String apiEndpoint;

    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String USERNAME = "Username";
        public static final String PASSWORD = "Password";
        public static final String API_URL = "Solr URL";
    }

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.USERNAME),
        new ConfigurableProperty(Properties.PASSWORD).setIsSensitive(true),
        new ConfigurableProperty(Properties.API_URL)
    );


    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public void initialize() throws BridgeError {
        this.username = properties.getValue(Properties.USERNAME);
        this.password = properties.getValue(Properties.PASSWORD);
        // Remove any trailing forward slash.
        this.apiEndpoint = properties.getValue(Properties.API_URL).replaceFirst("(\\/)$", "");
        testAuthenticationValues(this.apiEndpoint, this.username, this.password);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }

    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {

        SolrQualificationParser solrParser = new SolrQualificationParser();
        String jsonResponse = solrQuery("count", request, solrParser);
        Long count = JsonPath.parse(jsonResponse).read("$.response.numFound", Long.class);
        // Create and return a Count object.
        return new Count(count);

    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {

        SolrQualificationParser solrParser = new SolrQualificationParser();
        String metadataRoot = solrParser.getJsonRootPath(request.getQuery());
        String jsonRootPath = JSON_ROOT_DEFAULT;
        if (StringUtils.isNotBlank(metadataRoot)) {
            jsonRootPath = metadataRoot;
        }

        String jsonResponse = solrQuery("search", request, solrParser);
        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        Record recordResult = new Record(null);

        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            if (listRoot.size() == 1) {
                Map<String, Object> recordValues = new HashMap();
                for (String field : request.getFields()) {
                    try {
                        recordValues.put(field, JsonPath.parse(listRoot.get(0)).read(field));
                    } catch (InvalidPathException e) {
                        recordValues.put(field, null);
                    }
                }
                recordResult = new Record(recordValues);
            } else {
                throw new BridgeError("Multiple results matched an expected single match query");
            }
        } else if (objectRoot instanceof Map) {
            Map<String, Object> recordValues = new HashMap();
            for (String field : request.getFields()) {
                try {
                    recordValues.put(field, JsonPath.parse(objectRoot).read(field));
                } catch (InvalidPathException e) {
                    recordValues.put(field, null);
                }
            }
            recordResult = new Record(recordValues);
        }

        return recordResult;

    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {

        SolrQualificationParser solrParser = new SolrQualificationParser();
        String metadataRoot = solrParser.getJsonRootPath(request.getQuery());
        String jsonRootPath = JSON_ROOT_DEFAULT;
        if (StringUtils.isNotBlank(metadataRoot)) {
            jsonRootPath = metadataRoot;
        }

        String jsonResponse = solrQuery("search", request, solrParser);
        List<Record> recordList = new ArrayList<Record>();
        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        Map<String,String> metadata = new LinkedHashMap<String,String>();
        metadata.put("count",JsonPath.parse(jsonResponse).read("$.response.numFound", String.class));

        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            metadata.put("size", String.valueOf(listRoot.size()));
            for (Object arrayElement : listRoot) {
                Map<String, Object> recordValues = new HashMap();
                DocumentContext jsonObject = JsonPath.parse(arrayElement);
                if (request.getFields() != null) {
                    for (String field : request.getFields()) {
                        try {
                            recordValues.put(field, jsonObject.read(field));
                        } catch (InvalidPathException e) {
                            recordValues.put(field, null);
                        }
                    }
                }
                recordList.add(new Record(recordValues));
            }
        } else if (objectRoot instanceof Map) {
            metadata.put("size", "1");
            Map<String, Object> recordValues = new HashMap();
            DocumentContext jsonObject = JsonPath.parse(objectRoot);
            for (String field : request.getFields()) {
                recordValues.put(field, jsonObject.read(field));
            }
            recordList.add(new Record(recordValues));
        }

        return new RecordList(request.getFields(), recordList, metadata);

    }


    /*----------------------------------------------------------------------------------------------
     * PUBLIC HELPER METHODS
     *--------------------------------------------------------------------------------------------*/

    public String buildUrl(String queryMethod, BridgeRequest request) throws BridgeError {

        // Build up the url that you will use to retrieve the source data. Use the query variable
        // instead of request.getQuery() to post a query without parameter placeholders.
        StringBuilder url = new StringBuilder();

        Map<String,String> metadata = BridgeUtils.normalizePaginationMetadata(request.getMetadata());
        String pageSize = "1000";
        String offset = "0";

        if (StringUtils.isNotBlank(metadata.get("pageSize")) && metadata.get("pageSize").equals("0") == false) {
            pageSize = metadata.get("pageSize");
        }
        if (StringUtils.isNotBlank(metadata.get("offset"))) {
            offset = metadata.get("offset");
        }

        url.append(this.apiEndpoint)
            .append("/")
            .append(request.getStructure())
            .append("/select")
            .append("?wt=json");

        //Set row count to 0 if doing a count.
        if (queryMethod.equals("count")) {
            url.append("&rows=0");
        } else {
            url.append("&rows=" + pageSize)
                .append("&start=" + offset);
        }

        logger.trace("Solr URL: {}", url.toString());
        return url.toString();

    }

    public HttpEntity buildRequestBody(String queryMethod, BridgeRequest request, SolrQualificationParser solrParser) throws BridgeError {
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        HttpEntity result = null;

        String query = solrParser.parse(request.getQuery(),request.getParameters());
        //Set query to return everything if no qualification defined.
        if (StringUtils.isBlank(query)) {
            query = "*:*";
        }

        // If the query is a JSON object...
        if (query.matches("^\\s*\\{.*?\\}\\s*$")) {
            params.add(new BasicNameValuePair("json", query));
            logger.trace(String.format("JSON Query being sent to solr: %s", query));
        } else {
            params.add(new BasicNameValuePair("q", query));
            logger.trace(String.format("Lucene Query being sent to solr: %s", query));
        }

        //only set sorting and field return limitation if we're not counting.
        if (queryMethod.equals("count") == false) {

            //only set field limitation if we're not counting *and* the request specified fields to be returned.
            if (request.getFields() != null && request.getFields().isEmpty() == false) {
                StringBuilder includedFields = new StringBuilder();
                String[] bridgeFields = request.getFieldArray();
                for (int i = 0; i < request.getFieldArray().length; i++) {
                    //strip _source from the beginning of the specified field name as this is redundent to Solr.
                    includedFields.append(bridgeFields[i]);
                    //only append a comma if this is not the last field
                    if (i != (request.getFieldArray().length -1)) {
                        includedFields.append(",");
                    }
                }
                params.add(new BasicNameValuePair("fl", includedFields.toString()));
            }
            //only set sorting if we're not counting *and* the request specified a sort order.
            if (request.getMetadata("order") != null) {
                List<String> orderList = new ArrayList<String>();
                //loop over every defined sort order and add them to the Elasicsearch URL
                for (Map.Entry<String,String> entry : BridgeUtils.parseOrder(request.getMetadata("order")).entrySet()) {
                    String key = entry.getKey();
                    if (entry.getValue().equals("DESC")) {
                        orderList.add(String.format("%s desc", key));
                    }
                    else {
                        orderList.add(String.format("%s asc", key));
                    }
                }
                params.add(
                    new BasicNameValuePair(
                        "sort",
                        StringUtils.join(orderList,",")
                    )
                );
            }

        }

        try {
            result = new UrlEncodedFormEntity(params);
        } catch (UnsupportedEncodingException exceptionDetails) {
            throw new BridgeError (
                "Unable to generate the URL encoded HTTP Request body for the Solr API request.",
                exceptionDetails
            );
        }

        return result;
    }


    /*----------------------------------------------------------------------------------------------
     * PRIVATE HELPER METHODS
     *--------------------------------------------------------------------------------------------*/
    private void addBasicAuthenticationHeader(HttpRequestBase get, String username, String password) {
        String creds = String.format("%s:%s", username, password);
        byte[] basicAuthBytes = Base64.encodeBase64(creds.getBytes());
        get.setHeader("Authorization", String.format("Basic %s", new String(basicAuthBytes)));
    }

    private String solrQuery(String queryMethod, BridgeRequest request, SolrQualificationParser solrParser) throws BridgeError{

        String result = null;
        String url = buildUrl(queryMethod, request);

        // Initialize the HTTP Client, Response, and Get objects.
        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        HttpPost post = new HttpPost(url);

        // Append the authentication to the call. This example uses Basic Authentication but other
        // types can be added as HTTP GET or POST headers as well.
        if (this.username != null && this.password != null) {
            addBasicAuthenticationHeader(post, this.username, this.password);
        }

        post.setEntity(
            buildRequestBody(queryMethod, request, solrParser)
        );

        // Make the call to the REST source to retrieve data and convert the response from an
        // HttpEntity object into a Java string so more response parsing can be done.
        try {
            response = client.execute(post);
            Integer responseStatus = response.getStatusLine().getStatusCode();
            logger.trace(String.format("Request response code: %s", response.getStatusLine().getStatusCode()));

            if (responseStatus >= 300 || responseStatus < 200) {
                HttpEntity entity = response.getEntity();
                String errorMessage = EntityUtils.toString(entity);
                throw new BridgeError(
                    String.format(
                        "The Solr server returned a HTTP status code of %d, 200 was expected. Response body: %s",
                        responseStatus,
                        errorMessage
                    )
                );
            }

            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);

        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Solr server", e);
        }
        logger.trace(String.format("Solr response - Raw Output: %s", result));

        return result;
    }

    private void testAuthenticationValues(String restEndpoint, String username, String password) throws BridgeError {
        logger.debug("Testing the authentication credentials");
        HttpGet get = new HttpGet(String.format("%s/admin/cores?action=STATUS",restEndpoint));

        if (username != null && password != null) {
            addBasicAuthenticationHeader(get, this.username, this.password);
        }

        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
            Integer responseCode = response.getStatusLine().getStatusCode();
            if (responseCode == 401) {
                throw new BridgeError("Unauthorized: The inputted Username/Password combination is not valid.");
            }
            if (responseCode < 200 || responseCode >= 300) {
                throw new BridgeError(String.format("Unsuccessful HTTP response - the server returned a %s status code, expected 200.", responseCode));
            }
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Solr core status check API endpoint.", e);
        }
    }

}