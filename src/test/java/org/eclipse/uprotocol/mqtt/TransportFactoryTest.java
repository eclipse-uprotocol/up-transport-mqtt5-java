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

import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttClientIdentifier;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;

import org.eclipse.uprotocol.transport.UTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

class TransportFactoryTest {

    private Mqtt5AsyncClient mqttClient;

    @BeforeEach
    void createClient() {
        var config = mock(Mqtt5ClientConfig.class);
        when(config.getClientIdentifier()).thenReturn(Optional.of(MqttClientIdentifier.of("test.client")));
        mqttClient = mock(Mqtt5AsyncClient.class);
        when(mqttClient.getState()).thenReturn(MqttClientState.CONNECTED);
        when(mqttClient.getConfig()).thenReturn(config);
        when(mqttClient.toAsync()).thenReturn(mqttClient);
    }

    @Test
    void testFactoryRejectsNullArgs() {
        assertThatCode(() -> HiveMqTransportFactory.createInstance(
                null,
                TransportMode.IN_VEHICLE,
                "my.vehicle"))
                .isInstanceOf(NullPointerException.class);
        assertThatCode(() -> HiveMqTransportFactory.createInstance(
                mqttClient,
                TransportMode.IN_VEHICLE,
                null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testFactoryRejectsDisconnectedClient() {
        when(mqttClient.getState()).thenReturn(MqttClientState.DISCONNECTED);
        assertThatCode(() -> HiveMqTransportFactory.createInstance(
                mqttClient,
                TransportMode.IN_VEHICLE,
                "my.vehicle"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFactorySucceeds() {
        var result = HiveMqTransportFactory.createInstance(
            mqttClient,
            TransportMode.IN_VEHICLE,
            "my.vehicle");

        assertThat(result)
                .isInstanceOf(HiveMqTransport.class)
                .isInstanceOf(UTransport.class);
    }
}