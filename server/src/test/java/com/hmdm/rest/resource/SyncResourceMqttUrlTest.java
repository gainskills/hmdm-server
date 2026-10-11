package com.hmdm.rest.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.hmdm.notification.guice.module.NotificationMqttConfigModule;
import com.hmdm.event.EventService;
import com.hmdm.persistence.CustomerDAO;
import com.hmdm.persistence.DeviceDAO;
import com.hmdm.persistence.UnsecureDAO;
import com.hmdm.persistence.domain.Configuration;
import com.hmdm.persistence.domain.Customer;
import com.hmdm.persistence.domain.Device;
import com.hmdm.persistence.domain.Settings;
import com.hmdm.rest.json.Response;
import com.hmdm.util.CryptoUtil;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletContext;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SyncResourceMqttUrlTest {
    @Test
    public void includesDeploymentEndpointInSignedEnrollmentAndSyncWithEitherDesign() throws Exception {
        UnsecureDAO dao = mock(UnsecureDAO.class);
        CustomerDAO customers = mock(CustomerDAO.class);
        Device device = new Device();
        device.setId(1);
        device.setNumber("test-device");
        device.setCustomerId(1);
        device.setConfigurationId(1);
        Configuration configuration = new Configuration();
        configuration.setPassword("test-password");
        configuration.setApplicationSettings(Collections.emptyList());
        when(dao.getDeviceByNumber("test-device")).thenReturn(device);
        when(dao.getSettings(1)).thenReturn(new Settings());
        when(dao.getPlainConfigurationApplications(1, 1)).thenReturn(Collections.emptyList());
        when(dao.getConfigurationByIdWithAppSettings(1)).thenReturn(configuration);
        when(dao.getDeviceAppSettings(1)).thenReturn(Collections.emptyList());
        when(dao.getConfigurationFiles(device)).thenReturn(new ArrayList<>());
        when(customers.findById(1)).thenReturn(new Customer());

        for (String external : new String[]{null, "0", "1", "true"}) {
            String endpoint = "ssl://devices.example.com:8883";
            ServletContext context = mock(ServletContext.class);
            when(context.getInitParameter("mqtt.external")).thenReturn(external);
            when(context.getInitParameter("mqtt.server.uri")).thenReturn("mqtts://devices.example.com");
            String serverUri = Guice.createInjector(new NotificationMqttConfigModule(context))
                    .getInstance(Key.get(String.class, Names.named("mqtt.server.uri")));
            SyncResource resource = new SyncResource(dao, mock(EventService.class), Guice.createInjector(),
                    customers, mock(DeviceDAO.class), "https://mdm.example.com", serverUri,
                    false, "test-secret", false, "", "", "", "");
            for (boolean defaultDesign : new boolean[]{false, true}) {
                configuration.setUseDefaultDesignSettings(defaultDesign);
                for (boolean enrollment : new boolean[]{false, true}) {
                    HttpServletRequest request = mock(HttpServletRequest.class);
                    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
                    HttpServletResponse httpResponse = mock(HttpServletResponse.class);
                    Response response = enrollment
                            ? resource.enrollDevice(null, "test-device", request, httpResponse)
                            : resource.getDeviceSetting("test-device", request, httpResponse);
                    assertEquals(Response.ResponseStatus.OK, response.getStatus());
                    JsonNode json = new ObjectMapper().valueToTree(response.getData());
                    assertEquals(endpoint, json.get("mqttUrl").asText());
                    verify(httpResponse).setHeader("X-Response-Signature",
                            CryptoUtil.getDataSignature("test-secret", response.getData()));
                }
            }
        }
    }
}
