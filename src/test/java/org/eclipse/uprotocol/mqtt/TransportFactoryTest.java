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

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConnectionConfig;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.v1.UUri;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

class TransportFactoryTest {


    @Test
    void givenNoClient_whenInvokeCreateInstance_shouldThrowAnException() {
        assertThatCode(() -> TransportFactory.createInstance(mock(UUri.class), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("source and client must not be null");
    }

    @Test
    void givenValidClient_whenInvokeCreateInstance_shouldReturnInstanceOfUTransport() {
        Mqtt5Client mockedClient = mock(Mqtt5Client.class);
        Mqtt5ClientConfig mockedConfig = mock(Mqtt5ClientConfig.class);
        doReturn(mockedConfig).when(mockedClient).getConfig();
        doReturn(Optional.of(mock(Mqtt5ClientConnectionConfig.class))).when(mockedConfig).getConnectionConfig();

        var result = TransportFactory.createInstance(mock(UUri.class), mockedClient);

        assertThat(result)
                .isInstanceOf(HiveMqMQTT5Client.class)
                .isInstanceOf(UTransport.class);
    }

    @Test
    void givenNotConnectedClient_whenInvokeCreateInstance_shouldInvokeConnect() {
        Mqtt5Client mockedClient = mock(Mqtt5Client.class);
        doReturn(mock(Mqtt5ClientConfig.class)).when(mockedClient).getConfig();
        Mqtt5BlockingClient mockedInnerClient = mock(Mqtt5BlockingClient.class);
        doReturn(mockedInnerClient).when(mockedClient).toBlocking();

        var result = TransportFactory.createInstance(mock(UUri.class), mockedClient);

        assertThat(result)
                .isInstanceOf(HiveMqMQTT5Client.class)
                .isInstanceOf(UTransport.class);
        verify(mockedInnerClient).connect();
    }
}