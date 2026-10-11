package com.hmdm.notification;

import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.hmdm.notification.guice.module.NotificationMqttConfigModule;
import org.junit.Test;

import javax.servlet.ServletContext;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DeviceMqttUrlTest {
    @Test
    public void normalizesEndpointsAndDefaultPorts() {
        assertEquals("tcp://mqtt.example.com:31000", DeviceMqttUrl.normalize("tcp://mqtt.example.com:31000"));
        assertEquals("ssl://mqtt.example.com:8883", DeviceMqttUrl.normalize("ssl://mqtt.example.com"));
        assertEquals("tcp://mqtt.example.com:1883", DeviceMqttUrl.normalize(" MQTT://MQTT.EXAMPLE.COM "));
        assertEquals("ssl://mqtt.example.com:8883", DeviceMqttUrl.normalize("mqtts://mqtt.example.com"));
        assertEquals("tcp://[2001:db8::1]:1883", DeviceMqttUrl.normalize("tcp://[2001:db8::1]"));
    }

    @Test
    public void embeddedModeNormalizesSharedEndpointAtStartup() {
        for (String external : new String[]{null, "", "0", "false"}) {
            ServletContext context = mock(ServletContext.class);
            when(context.getInitParameter("mqtt.external")).thenReturn(external);
            when(context.getInitParameter("mqtt.server.uri")).thenReturn("mqtt://broker.example.com:31000");
            assertEquals("tcp://broker.example.com:31000", Guice.createInjector(new NotificationMqttConfigModule(context))
                    .getInstance(Key.get(String.class, Names.named("mqtt.server.uri"))));
        }
    }

    @Test
    public void rejectsExplicitInvalidEndpointsWithoutDisclosingCredentials() {
        String[] invalid = {null, "", " \t ", "ws://mqtt.example.com", "wss://mqtt.example.com", "https://mqtt.example.com",
                "mqtt.example.com:1883", "tcp:///", "tcp://:1883", "tcp://bad host:1883",
                "tcp://host:0", "ssl://host:65536", "tcp://host:-1", "tcp://host:",
                "tcp://host/path", "tcp://host/", "tcp://host?query", "tcp://host#fragment",
                "tcp://user:secret@host:1883"};
        for (String endpoint : invalid) {
            try {
                DeviceMqttUrl.normalize(endpoint);
                fail("Accepted invalid endpoint: " + endpoint);
            } catch (IllegalArgumentException expected) {
                assertFalse(expected.getMessage().contains("secret"));
                assertNull(expected.getCause());
            }
        }
    }

    @Test
    public void externalModeNormalizesSharedEndpointAtStartup() {
        for (String external : new String[]{"1", "true", "TRUE"}) {
            ServletContext context = mock(ServletContext.class);
            when(context.getInitParameter("mqtt.external")).thenReturn(external);
            when(context.getInitParameter("mqtt.server.uri")).thenReturn("mqtts://devices.example.com");
            assertEquals("ssl://devices.example.com:8883", Guice.createInjector(new NotificationMqttConfigModule(context))
                    .getInstance(Key.get(String.class, Names.named("mqtt.server.uri"))));
        }
    }

    @Test
    public void bothModesRejectMissingOrInvalidEndpointAtStartup() {
        for (String external : new String[]{null, "0", "1"}) {
            for (String endpoint : new String[]{null, "", "  ", "wss://devices.example.com", "tcp://host:0"}) {
                ServletContext context = mock(ServletContext.class);
                when(context.getInitParameter("mqtt.external")).thenReturn(external);
                when(context.getInitParameter("mqtt.server.uri")).thenReturn(endpoint);
                try {
                    Guice.createInjector(new NotificationMqttConfigModule(context));
                    fail("Invalid broker endpoint must fail injector creation");
                } catch (CreationException expected) {
                    assertTrue(expected.getMessage().contains("mqtt.server.uri"));
                }
            }
        }
    }
}
