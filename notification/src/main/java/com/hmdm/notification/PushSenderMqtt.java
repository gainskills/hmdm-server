package com.hmdm.notification;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.hmdm.notification.guice.module.NotificationMqttTaskModule;
import com.hmdm.notification.persistence.domain.PushMessage;
import com.hmdm.persistence.UnsecureDAO;
import com.hmdm.persistence.domain.Device;
import com.hmdm.util.BackgroundTaskRunnerService;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;

@Singleton
public class PushSenderMqtt implements PushSender {
    private final MqttUriUtil.MqttUri mqttUri;
    private final String clientTag;
    private final boolean mqttAuth;
    private final String mqttAdminPassword;
    private final String mqttExternal;
    private final UnsecureDAO unsecureDAO;
    private final MqttThrottledSender throttledSender;
    private final BackgroundTaskRunnerService taskRunner;
    private final MemoryPersistence persistence = new MemoryPersistence();
    private final long mqttDelay;
    private final String sslKeystorePassword;
    private final String sslProtocols;
    private MqttClient client;
    private static final Logger log = LoggerFactory.getLogger(PushSenderMqtt.class);

    @Inject
    public PushSenderMqtt(@Named("mqtt.server.uri") String serverUri,
            @Named("mqtt.client.tag") String clientTag,
            @Named("mqtt.auth") boolean mqttAuth,
            @Named("mqtt.admin.password") String mqttAdminPassword,
            @Named("mqtt.external") String mqttExternal,
            @Named("mqtt.message.delay") long mqttDelay,
            @Named("ssl.keystore.password") String sslKeystorePassword,
            @Named("ssl.protocols") String sslProtocols,
            MqttThrottledSender throttledSender,
            BackgroundTaskRunnerService taskRunner,
            UnsecureDAO unsecureDAO) {
        this.mqttUri = MqttUriUtil.parse(serverUri);
        this.clientTag = clientTag;
        this.mqttAuth = mqttAuth;
        this.mqttAdminPassword = mqttAdminPassword;
        this.mqttExternal = mqttExternal;
        this.mqttDelay = mqttDelay;
        this.sslKeystorePassword = sslKeystorePassword;
        this.sslProtocols = sslProtocols;
        this.throttledSender = throttledSender;
        this.taskRunner = taskRunner;
        this.unsecureDAO = unsecureDAO;
    }

    @Override
    public void init() {
        try {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);
            options.setKeepAliveInterval(600);
            if (mqttUri.isSecure()) {
                SSLSocketFactory sslSocketFactory = MqttUriUtil.configureSSL(mqttUri, sslKeystorePassword,
                        sslProtocols);
                if (sslSocketFactory == null) {
                    throw new IllegalStateException(
                            "Failed to create SSL socket factory for secure MQTT connection: " + mqttUri.toString());
                }
                options.setSocketFactory(sslSocketFactory);
            }
            if (mqttAuth) {
                options.setUserName(NotificationMqttTaskModule.MQTT_ADMIN_USERNAME);
                options.setPassword(mqttAdminPassword.toCharArray());
            }

            InetAddress address = InetAddress.getByName(mqttUri.getHost());
            log.info("Connecting to MQTT broker - Hostname: {}, Resolved IP: {}, Port: {}, Full URI: {}",
                    mqttUri.getHost(), address.getHostAddress(), mqttUri.getPort(), mqttUri.toString());

            client = new MqttClient(mqttUri.toString(), "HMDMServer" + clientTag, persistence);
            client.connect(options);
            if (mqttDelay > 0) {
                throttledSender.setClient(client);
                taskRunner.submitTask(throttledSender);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int send(PushMessage message) {
        if (client == null || !client.isConnected()) {
            // Not initialized
            return 0;
        }
        // Since this method is used by scheduled task service which is impersonated,
        // we use UnsecureDAO here (which doesn't check the signed user).
        Device device = unsecureDAO.getDeviceById(message.getDeviceId());
        if (device == null) {
            // We shouldn't be here!
            return 0;
        }
        try {
            String strMessage = "{messageType: \"" + message.getMessageType() + "\"";
            if (message.getPayload() != null) {
                strMessage += ", payload: " + message.getPayload();
            }
            strMessage += "}";

            MqttMessage mqttMessage = new MqttMessage(strMessage.getBytes());
            mqttMessage.setQos(2);
            String number = device.getOldNumber() == null ? device.getNumber() : device.getOldNumber();
            if (mqttDelay == 0) {
                client.publish(number, mqttMessage);
            } else {
                throttledSender.send(new MqttEnvelope(number, mqttMessage));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}