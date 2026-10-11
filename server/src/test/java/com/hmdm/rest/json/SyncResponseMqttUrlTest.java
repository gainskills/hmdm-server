package com.hmdm.rest.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class SyncResponseMqttUrlTest {
    @Test
    public void serializesCanonicalDeviceEndpointProperty() throws Exception {
        SyncResponse response = new SyncResponse();
        response.setMqttUrl("ssl://devices.example.com:8883");
        JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(response));
        assertEquals("ssl://devices.example.com:8883", json.get("mqttUrl").asText());
    }

    @Test
    public void omitsEndpointForCompatibilityDefaults() throws Exception {
        for (String endpoint : new String[]{null, "", "  "}) {
            SyncResponse response = new SyncResponse();
            response.setMqttUrl(endpoint);
            JsonNode json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(response));
            assertFalse(json.has("mqttUrl"));
        }
    }
}
