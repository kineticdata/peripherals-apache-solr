package com.kineticdata.bridgehub.adapter.solr;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

public class SolrAdapterTest {
    
    //solr start -e techproducts
    private final String apiUrl = "http://localhost:8983/solr";
    private final String structure = "techproducts";
    
    @Test
    public void testCountResults() throws Exception {
        Integer expectedCount = 3;
        String expectedUrl = String.format("%s/%s/select?wt=json&rows=0", apiUrl, structure);
        Count actualCount;
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL",apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("product name", "ipod");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure(structure);
        request.setQuery("name:<%= parameter[\"product name\"] %>");
        
        assertEquals(expectedUrl, adapter.buildUrl("count", request));
        
        try {
            actualCount = adapter.count(request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedCount, actualCount.getValue());
    }
    
    @Test
    public void testCountResults_solrJson() throws Exception {
        Integer expectedCount = 3;
        String expectedUrl = String.format("%s/%s/select?wt=json&rows=0", apiUrl, structure);
        Count actualCount;
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL",apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("product name", "ipod");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure(structure);
        request.setQuery("{\"type\": \"Solr DSL\", \"query\": \"{\\\"query\\\": \\\"name:*<%= parameter[\"product name\"] %>*\\\"}\"}");
        
        assertEquals(expectedUrl, adapter.buildUrl("count", request));
        
        try {
            actualCount = adapter.count(request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedCount, actualCount.getValue());
    }
    
    @Test
    public void testPaginationUrl() throws BridgeError{
        Integer pageSize = 25;
        Integer offset = 5;
        String expectedUrl = String.format("%s/%s/select?wt=json&rows=%s&start=%s", apiUrl, structure, pageSize, offset);
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL",apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", pageSize.toString());
        bridgeMetadata.put("offset", offset.toString());
        
        BridgeRequest request = new BridgeRequest();
        request.setMetadata(bridgeMetadata);
        request.setStructure(structure);
        request.setQuery("message:*test*");
        
        assertEquals(expectedUrl, adapter.buildUrl("search", request));
    }
    
    @Test
    public void testCountResults_kineticJson() throws Exception {
        Integer expectedCount = 1;
        String expectedUrl = String.format("%s/%s/select?wt=json&rows=0", apiUrl, structure);
        Count actualCount;
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL",apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("kinetic json query", "{\"name\": { \"value\": \"ipod\", \"matcher\": \"like\" }, \"features\": {\"value\": \"mp3\"} }");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure(structure);
        request.setQuery("{\"type\": \"Kinetic DSL\", \"query\": \"<%= parameter[\"kinetic json query\"] %>\"}");
        
        assertEquals(expectedUrl, adapter.buildUrl("count", request));
        
        try {
            actualCount = adapter.count(request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedCount, actualCount.getValue());
    }
    
    @Test
    public void testCountResults_kineticJsonWithFieldGroupMatching() throws Exception {
        Integer expectedCount = 1;
        String expectedUrl = String.format("%s/%s/select?wt=json&rows=0", apiUrl, structure);
        Count actualCount;
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL",apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("kinetic json query", "{\"name\": { \"value\": [\"Apple\", \"ipod\"], \"matcher\": \"like\", \"requireAll\": true} }");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure(structure);
        request.setQuery("{\"type\": \"Kinetic DSL\", \"query\": \"<%= parameter[\"kinetic json query\"] %>\"}");
        
        assertEquals(expectedUrl, adapter.buildUrl("count", request));
        
        try {
            actualCount = adapter.count(request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedCount, actualCount.getValue());
    }
    
    @Test
    public void testRetrieveResults() throws Exception {
        String expectedUrl = String.format("%s/%s/select?wt=json&rows=1000&start=0", apiUrl, structure);
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL", apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("product name", "GB18030");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);
        request.setStructure(structure);
        request.setQuery("name:<%= parameter[\"product name\"] %>");
        request.setFields(
            Arrays.asList(
                "name",
                "keywords"
            )
        );
        
        assertEquals(expectedUrl, adapter.buildUrl("search", request));
        
        Record bridgeRecord = adapter.retrieve(request);
        
    }
    
}
