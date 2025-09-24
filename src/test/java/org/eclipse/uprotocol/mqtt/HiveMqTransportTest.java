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
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttClientIdentifier;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.Mqtt5Unsubscribe;

import org.eclipse.uprotocol.communication.UPayload;
import org.eclipse.uprotocol.communication.UStatusException;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.builder.UMessageBuilder;
import org.eclipse.uprotocol.uuid.factory.UuidFactory;
import org.eclipse.uprotocol.v1.UAttributes;
import org.eclipse.uprotocol.v1.UCode;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UMessageType;
import org.eclipse.uprotocol.v1.UPayloadFormat;
import org.eclipse.uprotocol.v1.UUri;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HiveMqTransportTest {

    private final static UUri UURI_SOURCE = UUri.newBuilder()
            .setAuthorityName("my.vehicle")
            .setUeId(0xabcd)
            .setUeVersionMajor(0x01)
            .build();
    private final static UUri UURI_TOPIC = UUri.newBuilder(UURI_SOURCE)
            .setResourceId(0x9b3a)
            .build();
    private final static UUri UURI_DESTINATION = UUri.newBuilder()
            .setAuthorityName("other.vehicle")
            .setUeId(0x1_75a1)
            .setUeVersionMajor(0x02)
            .build();
    private final static UUri UURI_METHOD = UUri.newBuilder(UURI_DESTINATION)
            .setResourceId(0x7f21)
            .build();

    private Mqtt5AsyncClient mqttClient;
    private HiveMqTransport serviceUnderTest;

    @BeforeEach
    void setUp() {
        var config = mock(Mqtt5ClientConfig.class);
        when(config.getClientIdentifier()).thenReturn(Optional.of(MqttClientIdentifier.of("test.client")));
        mqttClient = mock(Mqtt5AsyncClient.class);
        when(mqttClient.getState()).thenReturn(MqttClientState.CONNECTED);
        when(mqttClient.getConfig()).thenReturn(config);
        when(mqttClient.toAsync()).thenReturn(mqttClient);
        when(mqttClient.publish(any(Mqtt5Publish.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        serviceUnderTest = new HiveMqTransport(mqttClient, UURI_SOURCE.getAuthorityName(), TransportMode.IN_VEHICLE);
    }

    @Test
    void testRegisterListenerFailsForNullArgs() {
        assertThrows(
                NullPointerException.class,
                () -> serviceUnderTest.registerListener(
                        null,
                        Optional.empty(),
                        mock(UListener.class)));
        assertThrows(
                NullPointerException.class,
                () -> serviceUnderTest.registerListener(
                        UURI_TOPIC,
                        null,
                        mock(UListener.class)));
        assertThrows(
                NullPointerException.class,
                () -> serviceUnderTest.registerListener(
                        UURI_TOPIC,
                        Optional.empty(),
                        null));
    }

    @Test
    void testRegisterListenerFailsForInvalidFilter() {
        
        var ex = assertThrows(
            CompletionException.class,
            () -> serviceUnderTest.registerListener(
                UURI_DESTINATION,
                Optional.of(UURI_DESTINATION),
                mock(UListener.class))
            .toCompletableFuture().join());
        assertInstanceOf(UStatusException.class, ex.getCause());
        assertEquals(UCode.INVALID_ARGUMENT, ((UStatusException) ex.getCause()).getCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRegisterListenerFailsForMqttError() {
        var subAck = mock(Mqtt5SubAck.class);
        when(mqttClient.subscribe(any(Mqtt5Subscribe.class), any(Consumer.class)))
            .thenReturn(CompletableFuture.failedFuture(new Mqtt5SubAckException(subAck, "cannot subscribe")));
        var ex = assertThrows(
                CompletionException.class,
                () -> serviceUnderTest.registerListener(
                            UURI_TOPIC,
                            Optional.empty(),
                            mock(UListener.class))
                        .toCompletableFuture().join());
        assertEquals(UCode.INTERNAL, ((UStatusException) ex.getCause()).getCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRegisterListenerSendsSubscribePacket() {
        var message = UMessageBuilder.publish(UURI_TOPIC).build();

        var listener = mock(UListener.class);
        doThrow(new RuntimeException("listener called")).when(listener).onReceive(message);
        ArgumentCaptor<Consumer<Mqtt5Publish>> captor = ArgumentCaptor.forClass(Consumer.class);
        when(mqttClient.subscribe(any(Mqtt5Subscribe.class), any(Consumer.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        serviceUnderTest.registerListener(
                UURI_TOPIC,
                Optional.empty(),
                listener)
            .toCompletableFuture()
            .join();
        verify(mqttClient).subscribe(any(Mqtt5Subscribe.class), captor.capture());
        var mqttPublish = Mapping.toMqtt5Publish(message, TransportMode.IN_VEHICLE, UURI_SOURCE.getAuthorityName());
        captor.getValue().accept(mqttPublish);
        verify(listener).onReceive(message);
    }

    @Test
    void testUnregisterListenerFailsForInvalidFilter() {

        var ex = assertThrows(
                CompletionException.class,
                () -> serviceUnderTest.unregisterListener(
                        UURI_DESTINATION,
                        Optional.of(UURI_DESTINATION),
                        mock(UListener.class))
                        .toCompletableFuture().join());
        assertInstanceOf(UStatusException.class, ex.getCause());
        assertEquals(UCode.INVALID_ARGUMENT, ((UStatusException) ex.getCause()).getCode());
    }

    @Test
    void testUnregisterListenerFailsForNullArgs() {
        assertThrows(
                NullPointerException.class,
                () -> serviceUnderTest.unregisterListener(
                        null,
                        Optional.empty(),
                        mock(UListener.class)));
        assertThrows(
                NullPointerException.class,
                () -> serviceUnderTest.unregisterListener(
                        UURI_TOPIC,
                        null,
                        mock(UListener.class)));
        assertThrows(
                NullPointerException.class,
                () -> serviceUnderTest.unregisterListener(
                        UURI_TOPIC,
                        Optional.empty(),
                        null));
    }

    @Test
    void testUnregisterListenerFailsForMqttError() {
        var subAck = mock(Mqtt5SubAck.class);
        when(mqttClient.unsubscribe(any(Mqtt5Unsubscribe.class)))
                .thenReturn(CompletableFuture.failedFuture(new Mqtt5SubAckException(subAck, "cannot unsubscribe")));
        var ex = assertThrows(
                CompletionException.class,
                () -> serviceUnderTest.unregisterListener(
                        UURI_TOPIC,
                        Optional.empty(),
                        mock(UListener.class))
                        .toCompletableFuture().join());
        assertEquals(UCode.INTERNAL, ((UStatusException) ex.getCause()).getCode());
    }

    @Test
    void testUnregisterSendsUnsubscribePacket() {
        var listener = mock(UListener.class);
        when(mqttClient.unsubscribe(any(Mqtt5Unsubscribe.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        serviceUnderTest.unregisterListener(
                UURI_TOPIC,
                Optional.empty(),
                listener)
            .toCompletableFuture()
            .join();
        verify(mqttClient).unsubscribe(any(Mqtt5Unsubscribe.class));
    }

    @Test
    void testSendFailsForNullArgs() {
        assertThrows(
                NullPointerException.class,
                () -> serviceUnderTest.send(null));
    }

    @Test
    void testSendFailsForInvalidMessage() {
        var invalidMessage = UMessage.newBuilder()
            .setAttributes(UAttributes.newBuilder()
                        .setType(UMessageType.UMESSAGE_TYPE_PUBLISH)
                        .setId(UuidFactory.create())
                        .setSource(UURI_DESTINATION) // invalid: not a topic
                        .build())
            .build();
        var ex = assertThrows(
                CompletionException.class,
                () -> serviceUnderTest.send(invalidMessage).toCompletableFuture().join());
        assertInstanceOf(UStatusException.class, ex.getCause());
        assertEquals(UCode.INVALID_ARGUMENT, ((UStatusException) ex.getCause()).getCode());
    }

    @Test
    void testSendFailsForMqttError() {
        var ack = mock(Mqtt5PubAck.class);
        when(mqttClient.publish(any(Mqtt5Publish.class)))
                .thenReturn(CompletableFuture.failedFuture(new Mqtt5PubAckException(ack, "cannot publish")));
        var ex = assertThrows(
                CompletionException.class,
                () -> serviceUnderTest.send(UMessageBuilder.publish(UURI_TOPIC).build())
                .toCompletableFuture().join());
        assertEquals(UCode.INTERNAL, ((UStatusException) ex.getCause()).getCode());
    }

    @Test
    void testSendPublishesToMqttClient() {
        var message = UMessageBuilder.publish(UURI_TOPIC)
            .build(UPayload.pack(ByteString.copyFromUtf8("hello"), UPayloadFormat.UPAYLOAD_FORMAT_TEXT));

        var captor = ArgumentCaptor.forClass(Mqtt5Publish.class);
        serviceUnderTest.send(message);
        verify(mqttClient).publish(captor.capture());
        assertThat(captor.getValue().getTopic().toString()).isEqualTo("my.vehicle/ABCD/0/1/9B3A");
    }
}