/**
 * SPDX-FileCopyrightText: 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.eclipse.uprotocol.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.uri.serializer.UriSerializer;
import org.eclipse.uprotocol.v1.UUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TransportFactory {
    private static final Logger log = LoggerFactory.getLogger(TransportFactory.class);

    /**
     * factory which receives a pre-build mqtt client
     * <p>
     * suggested method if you use something like spring-boot which creates mqttClient by configuration
     * @param source UUri of your uEntity
     * @param client mqttClient to connect to the mqtt broker
     * @return uTransport instance for mqtt
     */
    public static UTransport createInstance(UUri source, Mqtt5Client client) {
        if (source == null || client == null)
            throw new IllegalArgumentException("source and client must not be null");

        if (client.getConfig().getConnectionConfig().isEmpty()) {
            log.warn("client seems NOT to be connected. try to connect now");
            client.toBlocking().connect();
        }

        return new HiveMqMQTT5Client(source, client);
    }

    /**
     * create uTransport client with given parameters.
     * system default trust store, ciphers and protocols are used.
     * @param source UUri of your uEntity
     * @param serverHost hostname / domain of mqtt broker
     * @param port of mqtt broker
     * @return uTransport instance for mqtt
     */
    public static UTransport createInstanceWithSystemSSLConfig(UUri source, String serverHost, int port) {
        Mqtt5BlockingClient client = MqttClient.builder().useMqttVersion5()

                .identifier("uprotocol-mqtt-client-" + UriSerializer.serialize(source))
                .serverHost(serverHost)
                .serverPort(port)

                .sslWithDefaultConfig()

                .automaticReconnect()
                .initialDelay(1, TimeUnit.SECONDS)
                .applyAutomaticReconnect()

                .buildBlocking();

        client.connect();

        return new HiveMqMQTT5Client(source, client.toAsync());
    }

    /**
     * create uTransport client with given parameters
     * @param source UUri of your uEntity
     * @param serverHost hostname / domain of mqtt broker
     * @return uTransport instance for mqtt
     */
    public static UTransport createInstanceWithSystemSSLConfig(UUri source, String serverHost) {
        return createInstanceWithSystemSSLConfig(source, serverHost, 1883);
    }
}
