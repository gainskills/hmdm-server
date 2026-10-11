package com.hmdm.notification;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/** Validates the broker URI shared by the server publisher and Android devices. */
public final class DeviceMqttUrl {
    private DeviceMqttUrl() {
    }

    /** Returns a normalized TCP/TLS URL; both broker modes require an explicit endpoint. */
    public static String normalize(String configured) {
        if (configured == null || configured.trim().isEmpty()) {
            throw new IllegalArgumentException("mqtt.server.uri is required");
        }
        // Do not include the supplied URL in errors: rejected URLs may contain credentials.
        final String error = "Invalid mqtt.server.uri: expected tcp:// or ssl:// with a host and optional port"
                + " (mqtt:// and mqtts:// aliases allowed), without credentials, path, query or fragment";
        try {
            URI uri = new URI(configured.trim());
            String scheme = uri.getScheme();
            if ("tcp".equalsIgnoreCase(scheme) || "mqtt".equalsIgnoreCase(scheme)) {
                scheme = "tcp";
            } else if ("ssl".equalsIgnoreCase(scheme) || "mqtts".equalsIgnoreCase(scheme)) {
                scheme = "ssl";
            } else {
                throw new IllegalArgumentException(error);
            }
            String host = uri.getHost();
            int port = uri.getPort();
            if (port == -1) {
                port = "ssl".equals(scheme) ? 8883 : 1883;
            }
            if (host == null || host.isEmpty() || port < 1 || port > 65535
                    || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                    || (uri.getRawPath() != null && !uri.getRawPath().isEmpty())
                    || (uri.getRawAuthority() != null && uri.getRawAuthority().endsWith(":"))) {
                throw new IllegalArgumentException(error);
            }
            return scheme + "://" + host.toLowerCase(Locale.ROOT) + ":" + port;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(error);
        }
    }
}
