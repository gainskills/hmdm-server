package com.hmdm.notification.guice.module;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hmdm.notification.MqttUriUtil;
import com.hmdm.notification.PushSender;
import com.hmdm.util.CryptoUtil;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class NotificationMqttTaskModule {

    private String serverUri;
    private String mqttExternal;
    private boolean mqttAuth;
    private String mqttAdminPassword;
    private String sslKeystorePassword;
    private String sslProtocols;
    private String hashSecret;
    private BrokerService brokerService;
    private PushSender pushSender;
    private static final Logger log = LoggerFactory.getLogger(NotificationMqttTaskModule.class);
    public static final String MQTT_USERNAME = "hmdm";
    public static final String MQTT_ADMIN_USERNAME = "admin";

    @Inject
    public NotificationMqttTaskModule(@Named("mqtt.server.uri") String serverUri,
            @Named("mqtt.external") String mqttExternal,
            @Named("mqtt.auth") boolean mqttAuth,
            @Named("mqtt.admin.password") String mqttAdminPassword,
            @Named("ssl.keystore.password") String sslKeystorePassword,
            @Named("ssl.protocols") String sslProtocols,
            @Named("hash.secret") String hashSecret,
            @Named("MQTT") PushSender pushSender) {
        this.serverUri = serverUri;
        this.mqttExternal = mqttExternal;
        this.pushSender = pushSender;
        this.mqttAuth = mqttAuth;
        this.mqttAdminPassword = mqttAdminPassword;
        this.sslKeystorePassword = sslKeystorePassword;
        this.sslProtocols = sslProtocols;
        this.hashSecret = hashSecret;
    }

    /**
     * <p>
     * Creates the broker service
     * </p>
     */
    public void init() {
        if (!initBrokerService()) {
            return;
        }
        pushSender.init();
    }

    private boolean initBrokerService() {
        MqttUriUtil.MqttUri mqttUri = MqttUriUtil.parse(serverUri);
        if (MqttUriUtil.isExternalEnabled(mqttExternal)) {
            if (serverUri.equals("")) {
                log.error("external MQTT server is empty");
                return false;
            }
            log.info("MQTT service not started, use external MQTT server {}", serverUri);
            return true;
        }

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);

        if (mqttAuth) {
            SimpleAuthenticationPlugin authPlugin = new SimpleAuthenticationPlugin();
            authPlugin.setAnonymousAccessAllowed(false);
            AuthenticationUser user = new AuthenticationUser(MQTT_USERNAME,
                    CryptoUtil.getSHA1String(MQTT_USERNAME + hashSecret), "users");
            AuthenticationUser admin = new AuthenticationUser(MQTT_ADMIN_USERNAME, mqttAdminPassword, "admins");
            List<AuthenticationUser> users = new LinkedList<>();
            users.add(user);
            users.add(admin);
            authPlugin.setUsers(users);

            AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin();
            try {
                List<DestinationMapEntry> entries = new LinkedList<>();

                AuthorizationEntry authorizationEntry = new AuthorizationEntry();
                authorizationEntry.setTopic(">");
                authorizationEntry.setRead("users,admins");
                authorizationEntry.setWrite("admins");
                authorizationEntry.setAdmin("users,admins");
                entries.add(authorizationEntry);

                authorizationEntry = new AuthorizationEntry();
                authorizationEntry.setTopic("ActiveMQ.Advisory.>");
                authorizationEntry.setRead("users,admins");
                authorizationEntry.setWrite("users,admins");
                authorizationEntry.setAdmin("users,admins");
                entries.add(authorizationEntry);

                AuthorizationMap authorizationMap = new DefaultAuthorizationMap(entries);
                authorizationPlugin.setMap(authorizationMap);
            } catch (Exception e) {
                log.error("Failed to configure MQTT authorization", e);
            }
            brokerService.setPlugins(new BrokerPlugin[] { authPlugin, authorizationPlugin });
        }

        if (mqttUri.isSecure()) {
            try {
                MqttUriUtil.configureSSL(mqttUri, sslKeystorePassword, sslProtocols);
            } catch (Exception e) {
                log.error("Failed to validate SSL configuration, cannot start MQTT broker", e);
                return false;
            }
        }

        String protocol = mqttUri.isSecure() ? "mqtt+nio+ssl" : "mqtt+nio";
        String connectorUri = protocol + "://0.0.0.0:" + mqttUri.getPort();

        log.info("Starting embedded MQTT broker with connector: {}", connectorUri);
        try {
            brokerService.addConnector(connectorUri);
            brokerService.start();
        } catch (Exception e) {
            log.error("Failed to start MQTT broker: " + connectorUri, e);
            return false;
        }
        return true;
    }
}
