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

import com.google.protobuf.ByteString;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import org.eclipse.uprotocol.communication.UPayload;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.builder.UMessageBuilder;
import org.eclipse.uprotocol.uri.factory.UriFactory;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UPayloadFormat;
import org.eclipse.uprotocol.v1.UPriority;
import org.eclipse.uprotocol.v1.UUri;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Testcontainers
@SuppressWarnings("PMD.ClassNamingConventions")
class PublishSubscribeIT {

    private static final DockerImageName IMAGE_NAME_MOSQUITTO = DockerImageName
        .parse("eclipse-mosquitto")
        .withTag("2.0");

    private final static Logger LOGGER = LoggerFactory.getLogger(PublishSubscribeIT.class);

    private final static UUri UURI_SOURCE = UUri.newBuilder()
            .setAuthorityName("source.vehicle")
            .setUeId(0xabcd)
            .setUeVersionMajor(0x01)
            .build();
    private final static UUri UURI_TOPIC = UUri.newBuilder(UURI_SOURCE)
            .setResourceId(0x9b3a)
            .build();
    private final static UUri UURI_DESTINATION = UUri.newBuilder()
            .setAuthorityName("sink.vehicle")
            .setUeId(0x1_75a1)
            .setUeVersionMajor(0x02)
            .build();

    private GenericContainer<?> mqttBroker;

    @AfterEach
    void stopBroker() {
        if (mqttBroker != null) {
            mqttBroker.stop();
        }
    }

    /**
     * Starts an Eclipse Mosquitto Docker container based on given configuration
     * options.
     * <p>
     * All configuration is written to temporary files which are then bind-mounted into
     * the container.
     *
     * @param config     Additional Mosquitto configuration options.
     *                   <p>
     *                   The given configuration will be appended to the default
     *                   Mosquitto
     *                   configuration which is:
     *                   ```text
     *                   listener 1883
     *                   ```
     * @param passwords  Credentials to include in the Mosquitto configuration.
     *                   If {@code null}, the broker will allow anonymous access.
     * @param aclEntries Access Control List definitions to include in the Mosquitto
     *                   configuration.
     * @param mappedPort The host port to map the Mosquitto broker's container port
     *                   to. If empty, the broker will be mapped to an ephemeral
     *                   port on the host.
     * @throws IOException If the configuration file cannot be created or written.
     */
    @SuppressWarnings("resource")
    void startMosquitto(
        String config,
        String passwords,
        String aclEntries,
        OptionalInt mappedPort
    ) throws IOException {
        final Path mosquittoConfig = java.nio.file.Files.createTempFile(
            "uprotocol",
            "mosquitto_config");
        java.nio.file.Files.writeString(mosquittoConfig, "listener 1883\n", java.nio.file.StandardOpenOption.APPEND);
        if (config != null && !config.isEmpty()) {
            java.nio.file.Files.writeString(mosquittoConfig, config + "\n", java.nio.file.StandardOpenOption.APPEND);
        }

        Path mosquittoPasswords = java.nio.file.Files.createTempFile(
            "uprotocol",
            "mosquitto_passwords");
        if (passwords != null && !passwords.isEmpty()) {
            java.nio.file.Files.writeString(
                mosquittoPasswords,
                passwords + "\n",
                java.nio.file.StandardOpenOption.APPEND);
            java.nio.file.Files.writeString(mosquittoConfig, "allow_anonymous false\n",
                    java.nio.file.StandardOpenOption.APPEND);
            java.nio.file.Files.writeString(mosquittoConfig, "password_file /mosquitto/config/passwords\n",
                    java.nio.file.StandardOpenOption.APPEND);
        } else {
            java.nio.file.Files.writeString(mosquittoConfig, "allow_anonymous true\n",
                    java.nio.file.StandardOpenOption.APPEND);
        }

        Path mosquittoAcls = java.nio.file.Files.createTempFile("uprotocol", "mosquitto_acls");
        if (aclEntries != null && !aclEntries.isEmpty()) {
            java.nio.file.Files.writeString(mosquittoAcls, aclEntries + "\n", java.nio.file.StandardOpenOption.APPEND);
            java.nio.file.Files.writeString(
                    mosquittoConfig,
                    "acl_file /mosquitto/config/acls\n",
                    java.nio.file.StandardOpenOption.APPEND);
        }


        int containerPort = 1883;
        // let config_path = mosquittoConfig.into_temp_path();
        // let password_path = mosquitto_passwords.into_temp_path();
        // let acl_path = mosquitto_acls.into_temp_path();
        var logConsumer = new Slf4jLogConsumer(LOGGER);
        GenericContainer<?> container = new GenericContainer<>(IMAGE_NAME_MOSQUITTO)
                .withExposedPorts(containerPort)
                // .waitingFor(Wait.forLogMessage("^mosquitto\\ version\\ .*\\ running$", 1))
                .withFileSystemBind(
                    mosquittoConfig.toAbsolutePath().toString(),
                    "/mosquitto/config/mosquitto.conf",
                    BindMode.READ_WRITE)
                .withFileSystemBind(
                    mosquittoPasswords.toAbsolutePath().toString(),
                    "/mosquitto/config/passwords",
                    BindMode.READ_WRITE)
                .withFileSystemBind(
                    mosquittoAcls.toAbsolutePath().toString(),
                    "/mosquitto/config/acls",
                    BindMode.READ_WRITE);
        container.start();
        container.followOutput(logConsumer);
        mqttBroker = container;
    }

    @Test
    void testPublishSubscribeSucceeds() throws InterruptedException, IOException {
        startMosquitto(null, null, null, OptionalInt.empty());

        var subscriberClientId = "subscriber.client." + UUID.randomUUID();
        var subscriberClient = Mqtt5Client
                .builder()
                .automaticReconnectWithDefaultConfig()
                .identifier(subscriberClientId)
                .serverPort(mqttBroker.getMappedPort(1883))
                .serverHost(mqttBroker.getHost())
                .buildAsync();  
        subscriberClient.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(60)
            .send()
            .join();

        var listener = mock(UListener.class);
        var subscriber = HiveMqTransportFactory.createInstance(
                subscriberClient,
                TransportMode.IN_VEHICLE,
                UURI_DESTINATION.getAuthorityName());
        subscriber.registerListener(
                UriFactory.ANY,
                Optional.of(UURI_DESTINATION),
                listener)
            .toCompletableFuture().join();

        var publisherClientId = "publisher.client." + UUID.randomUUID();
        var publisherClient = Mqtt5Client
                .builder()
                .automaticReconnectWithDefaultConfig()
                .identifier(publisherClientId)
                .serverPort(mqttBroker.getMappedPort(1883))
                .serverHost(mqttBroker.getHost())
                .buildAsync();
        publisherClient.connectWith()
            .cleanStart(false)
            .sessionExpiryInterval(60)
            .send()
            .join();

        var receiveLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            receiveLatch.countDown();
            return null;
        }).when(listener).onReceive(any(UMessage.class));

        var message = UMessageBuilder.notification(UURI_TOPIC, UURI_DESTINATION)
                .withTtl(5315)
                .withTraceparent("someTraceParent")
                .withPriority(UPriority.UPRIORITY_CS2)
                .build(UPayload.pack(
                        ByteString.copyFromUtf8("Hello World"),
                        UPayloadFormat.UPAYLOAD_FORMAT_TEXT));
        var publisher = HiveMqTransportFactory.createInstance(
                publisherClient,
                TransportMode.IN_VEHICLE,
                UURI_SOURCE.getAuthorityName());
        publisher.send(message).toCompletableFuture().join();
        assertThat(receiveLatch.await(2, TimeUnit.SECONDS)).isTrue();
        ArgumentCaptor<UMessage> captor = ArgumentCaptor.forClass(UMessage.class);
        verify(listener).onReceive(captor.capture());
        assertThat(captor.getValue()).isEqualTo(message);
    }
}
