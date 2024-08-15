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
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.eclipse.uprotocol.communication.UPayload;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.transport.builder.UMessageBuilder;
import org.eclipse.uprotocol.uri.serializer.UriSerializer;
import org.eclipse.uprotocol.v1.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Testcontainers
class HiveMqIntegratedTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(HiveMqIntegratedTest.class);
    @Container
    final HiveMQContainer hivemqCe = new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce").withTag("2024.3"))
            .withLogLevel(Level.TRACE);
    private UTransport serviceUnderTest;
    private Mqtt5BlockingClient.Mqtt5Publishes handleToReceiveMqttMessages;
    private Mqtt5BlockingClient mqttClientForTests;


    @BeforeEach
    void setUp() {
        LOGGER.info("Hivemq started, setting up test state");
        Mqtt5BlockingClient client = Mqtt5Client
                .builder()
                .serverPort(hivemqCe.getMqttPort())
                .serverHost(hivemqCe.getHost())
                .buildBlocking();
        client.connect();

        mqttClientForTests = Mqtt5Client
                .builder()
                .serverPort(hivemqCe.getMqttPort())
                .serverHost(hivemqCe.getHost())
                .buildBlocking();
        handleToReceiveMqttMessages = mqttClientForTests.publishes(MqttGlobalPublishFilter.ALL);
        mqttClientForTests.connect();
        mqttClientForTests.subscribeWith().topicFilter().multiLevelWildcard().applyTopicFilter().send();

        UUri source = mock(UUri.class);
        serviceUnderTest = TransportFactory.createInstance(source, client);
    }

    @Test
    void givenValidClientAndMessage_whenInvokeSend_shouldSendCorrectMessageToMqtt() throws InterruptedException {
        UMessage message = UMessageBuilder.request(
                UUri.newBuilder().setAuthorityName("testSource.someUri.network").setUeId(2).setUeVersionMajor(1).setResourceId(0).build(),
                UUri.newBuilder().setAuthorityName("testDestination.someUri.network").setUeId(2).setUeVersionMajor(1).setResourceId(1).build(), 500)
                .withToken("SomeToken")
                .withTraceparent("someTraceParent")
                .build(new UPayload(ByteString.copyFrom("Hello World", Charset.defaultCharset()), UPayloadFormat.UPAYLOAD_FORMAT_TEXT));

        UStatus response = serviceUnderTest.send(message).toCompletableFuture().join();
        assertThat(response.getCode()).isEqualTo(UCode.OK);

        Mqtt5Publish receive = handleToReceiveMqttMessages.receive(1, TimeUnit.SECONDS).orElseThrow();
        assertThat(new String(receive.getPayloadAsBytes())).isEqualTo("Hello World");
    }

    @Test
    void givenValidClientAndSmallestMessage_whenInvokeSend_shouldSendCorrectMessageToMqtt() throws InterruptedException {
        UMessage message = UMessageBuilder.request(
                        UUri.newBuilder().setAuthorityName("testSource.someUri.network").setUeId(2).setUeVersionMajor(1).setResourceId(0).build(),
                        UUri.newBuilder().setAuthorityName("testDestination.someUri.network").setUeId(2).setUeVersionMajor(1).setResourceId(1).build(), 500)
                .build(new UPayload(ByteString.copyFrom("Hello World", Charset.defaultCharset()), UPayloadFormat.UPAYLOAD_FORMAT_TEXT));

        UStatus response = serviceUnderTest.send(message).toCompletableFuture().join();
        assertThat(response.getCode()).isEqualTo(UCode.OK);
        Mqtt5Publish receive = handleToReceiveMqttMessages.receive(1, TimeUnit.SECONDS).get();
        assertThat(new String(receive.getPayloadAsBytes())).isEqualTo("Hello World");
    }

    @Test
    void givenValidClientAndBroadcastMessage_whenInvokeSend_shouldSendCorrectMessageToMqtt() throws InterruptedException {
        UMessage message = UMessageBuilder.publish(UUri.newBuilder().setAuthorityName("testSource.someUri.network").setUeId(2).setUeVersionMajor(1).setResourceId(0x8001).build())
                .build(new UPayload(ByteString.copyFrom("Hello World", Charset.defaultCharset()), UPayloadFormat.UPAYLOAD_FORMAT_TEXT));


        UStatus response = serviceUnderTest.send(message).toCompletableFuture().join();
        assertThat(response.getCode()).isEqualTo(UCode.OK);
        Mqtt5Publish receive = handleToReceiveMqttMessages.receive(1, TimeUnit.SECONDS).get();
        assertThat(new String(receive.getPayloadAsBytes())).isEqualTo("Hello World");
        assertThat(receive.getTopic().toString()).isEqualTo("d/testSource.someUri.network/0002/01/8001");
    }

    @Test
    void givenBlancoListener_whenAddingListenerAndReceivingMessages_shouldCallListener() {
        UListener listener = mock(UListener.class);

        UStatus status = serviceUnderTest.registerListener(null, listener).toCompletableFuture().join();

        mqttClientForTests.publishWith().topic("a/some-source/c/d/e/some-sink/a/b/c")
                .userProperties(Mqtt5UserProperties.of(
                        Mqtt5UserProperty.of(String.valueOf(UAttributes.SOURCE_FIELD_NUMBER), UriSerializer.serialize(UUri.newBuilder().setAuthorityName("testSource.someUri.network").setUeId(2).setUeVersionMajor(1).setResourceId(0).build())),
                        Mqtt5UserProperty.of(String.valueOf(UAttributes.SINK_FIELD_NUMBER), UriSerializer.serialize(UUri.newBuilder().setAuthorityName("testDestination.someUri.network").setUeId(2).setUeVersionMajor(1).setResourceId(1).build()))
                ))
                .payload("Hello World".getBytes(Charset.defaultCharset()))
                .send();

        assertThat(status.getCode()).isEqualTo(UCode.OK);

        ArgumentCaptor<UMessage> captor = ArgumentCaptor.captor();
        verify(listener, Mockito.timeout(1000).times(1)).onReceive(captor.capture());
        assertThat(captor.getValue()).isNotNull();
        assertThat(captor.getValue().getAttributes().getSink().getAuthorityName()).isEqualTo("testDestination.someUri.network");
        assertThat(captor.getValue().getAttributes().getSource().getAuthorityName()).isEqualTo("testSource.someUri.network");
        assertThat(captor.getValue().getPayload()).isNotNull();
        assertThat(captor.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");
    }

    @Test
    void givenBlancoListener_whenAddingListenerAndReceivingAnyMessage_shouldCallListener() {
        UListener listener = mock(UListener.class);

        UStatus status = serviceUnderTest.registerListener(null, listener).toCompletableFuture().join();

        mqttClientForTests.publishWith().topic("a/b/c/d/e/f/a/b/c").payload("Hello World".getBytes(Charset.defaultCharset())).send();

        assertThat(status.getCode()).isEqualTo(UCode.OK);

        ArgumentCaptor<UMessage> captor = ArgumentCaptor.captor();
        verify(listener, Mockito.timeout(1000).times(1)).onReceive(captor.capture());
        assertThat(captor.getValue()).isNotNull();
        assertThat(captor.getValue().getPayload()).isNotNull();
        assertThat(captor.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");
    }

    @Test
    void givenBlancoListener_whenAddingListenerAndReceivingBroadcastMessages_shouldCallListener() {
        UListener listener = mock(UListener.class);

        UStatus status = serviceUnderTest.registerListener(null, listener).toCompletableFuture().join();

        mqttClientForTests.publishWith().topic("a/some-source/c/d/e").payload("Hello World".getBytes(Charset.defaultCharset())).send();

        assertThat(status.getCode()).isEqualTo(UCode.OK);

        ArgumentCaptor<UMessage> captor = ArgumentCaptor.captor();
        verify(listener, Mockito.timeout(1000).times(1)).onReceive(captor.capture());
        assertThat(captor.getValue()).isNotNull();
        assertThat(captor.getValue().getPayload()).isNotNull();
        assertThat(captor.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");
    }

    @Test
    void givenCloudListener_whenAddingListenerAndReceivingMessages_shouldCallListener() {
        UListener listener = mock(UListener.class);

        UStatus status = serviceUnderTest.registerListener(
                UUri.newBuilder()
                        .setAuthorityName("cloud")
                        .setUeId(0xffff)
                        .setUeVersionMajor(0xff)
                        .setResourceId(0xffff)
                        .build(), listener).toCompletableFuture().join();

        mqttClientForTests.publishWith().topic("c/cloud/c/d/e/f/a/b/c")
                .payload("Hello World".getBytes(Charset.defaultCharset()))
                .userProperties()
                .add("0","1")
                .applyUserProperties()
                .send();

        assertThat(status.getCode()).isEqualTo(UCode.OK);

        ArgumentCaptor<UMessage> captor = ArgumentCaptor.captor();
        verify(listener, Mockito.timeout(1000).times(1)).onReceive(captor.capture());
        assertThat(captor.getValue()).isNotNull();
        assertThat(captor.getValue().getPayload()).isNotNull();
        assertThat(captor.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");
    }

    @Test
    void givenBlancoListenerRegistered_whenUnregisterListener_shouldNotCallListenerOnReceivingMessages() {
        UListener listener = mock(UListener.class);
        serviceUnderTest.registerListener(null, listener).toCompletableFuture().join();

        UStatus status = serviceUnderTest.unregisterListener(null, listener).toCompletableFuture().join();

        mqttClientForTests.publishWith().topic("a/b/c/d/e/f/a/b/c").payload("Hello World".getBytes(Charset.defaultCharset())).send();

        assertThat(status.getCode()).isEqualTo(UCode.OK);

        verify(listener, Mockito.timeout(1000).times(0)).onReceive(any());
    }

    @Test
    void given2ListenersForSameSourceAndSink_whenReceivingMessages_shouldInvokeBothListeners() {
        UListener listener = mock(UListener.class);
        UListener listener2 = mock(UListener.class);
        serviceUnderTest.registerListener(null, listener).toCompletableFuture().join();
        serviceUnderTest.registerListener(null, listener2).toCompletableFuture().join();

        mqttClientForTests.publishWith().topic("a/b/c/d/e/f/a/b/c").payload("Hello World".getBytes(Charset.defaultCharset())).send();

        ArgumentCaptor<UMessage> captor = ArgumentCaptor.captor();
        verify(listener, Mockito.timeout(1000).times(1)).onReceive(captor.capture());
        assertThat(captor.getValue()).isNotNull();
        assertThat(captor.getValue().getPayload()).isNotNull();
        assertThat(captor.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");

        ArgumentCaptor<UMessage> captor2 = ArgumentCaptor.captor();
        verify(listener2, Mockito.timeout(1000).times(1)).onReceive(captor2.capture());
        assertThat(captor2.getValue()).isNotNull();
        assertThat(captor2.getValue().getPayload()).isNotNull();
        assertThat(captor2.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");
    }

    @Disabled("Currently not supported. If one listener is unsubscribed, both loose the linkage")
    @Test
    void given2ListenersForSameSourceAndSink_whenUnregisterOneListener_shouldInvokeOtherListenersOnMessageReceived() {
        UListener radioDeviceListenOnCloudEvents = mock(UListener.class);
        UListener multimediaDeviceListenOnCloudEvents = mock(UListener.class);
        serviceUnderTest.registerListener(UUri.newBuilder()
                .setAuthorityName("cloud")
                .setUeId(0xffff)
                .setUeVersionMajor(0xff)
                .setResourceId(0xffff)
                .build(),
                radioDeviceListenOnCloudEvents).toCompletableFuture().join();
        serviceUnderTest.registerListener(UUri.newBuilder()
                .setAuthorityName("cloud")
                .setUeId(0xffff)
                .setUeVersionMajor(0xff)
                .setResourceId(0xffff)
                .build(),
                multimediaDeviceListenOnCloudEvents).toCompletableFuture().join();


        //radio went offline, caused by human pressing on power button on radio
        serviceUnderTest.unregisterListener(UUri.newBuilder()
                        .setAuthorityName("cloud")
                        .setUeId(0xffff)
                        .setUeVersionMajor(0xff)
                        .setResourceId(0xffff)
                        .build(),
                radioDeviceListenOnCloudEvents).toCompletableFuture().join();

        //some service publishes something on cloud
        mqttClientForTests.publishWith().topic("c/cloud/c/d/e/f/a/b/c").payload("Hello World".getBytes(Charset.defaultCharset())).send();

        ArgumentCaptor<UMessage> multiMediaCapture = ArgumentCaptor.captor();
        verify(multimediaDeviceListenOnCloudEvents, Mockito.timeout(1000).times(1)).onReceive(multiMediaCapture.capture());
        assertThat(multiMediaCapture.getValue()).isNotNull();
        assertThat(multiMediaCapture.getValue().getPayload()).isNotNull();
        assertThat(multiMediaCapture.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");

        verify(radioDeviceListenOnCloudEvents, Mockito.timeout(1000).times(0)).onReceive(any());
    }

    @Test
    void given2Listeners_whenUnregisterOneListener_shouldInvokeOtherListenersOnMessageReceived() {
        UListener allWildcardListener = mock(UListener.class);
        UListener listenerForAllCloudEvents = mock(UListener.class);
        serviceUnderTest.registerListener(null, allWildcardListener).toCompletableFuture().join();
        serviceUnderTest.registerListener(UUri.newBuilder()
                        .setAuthorityName("cloud")
                        .setUeId(0xffff)
                        .setUeVersionMajor(0xff)
                        .setResourceId(0xffff)
                        .build(),
                listenerForAllCloudEvents).toCompletableFuture().join();

        serviceUnderTest.unregisterListener(UUri.newBuilder()
                        .setAuthorityName("cloud")
                        .setUeId(0xffff)
                        .setUeVersionMajor(0xff)
                        .setResourceId(0xffff)
                        .build(),
                listenerForAllCloudEvents).toCompletableFuture().join();

        mqttClientForTests.publishWith().topic("c/cloud/c/d/e/f/a/b/c").payload("Hello World".getBytes(Charset.defaultCharset())).send();

        ArgumentCaptor<UMessage> captor = ArgumentCaptor.captor();
        verify(allWildcardListener, Mockito.timeout(1000).times(1)).onReceive(captor.capture());
        assertThat(captor.getValue()).isNotNull();
        assertThat(captor.getValue().getPayload()).isNotNull();
        assertThat(captor.getValue().getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");

        verify(listenerForAllCloudEvents, Mockito.timeout(1000).times(0)).onReceive(any());
    }

    @Test
    void givenListener_whenCloudSendsMessageToRadioAndRadioListens_shouldRouteMessageToRadio() {
        //given a radio and a cloudService
        UListener radioListener = mock(UListener.class);
        UUri radioUuid = UUri.newBuilder()
                .setAuthorityName("radio")
                .setUeId(0xffff)
                .setUeVersionMajor(0xff)
                .setResourceId(0)
                .build();
        UTransport mqttClientOfRadio = TransportFactory.createInstance(radioUuid, mqttClientForTests);

        UUri cloudService = UUri.newBuilder()
                .setAuthorityName("cloud")
                .setUeId(0xffff)
                .setUeVersionMajor(0xff)
                .setResourceId(0xffff)
                .build();
        UTransport mqttClientOfCloud = TransportFactory.createInstance(cloudService, mqttClientForTests);

        mqttClientOfRadio.registerListener(cloudService, radioUuid, radioListener);

        //when cloud service sends a message

        UMessage message = UMessageBuilder.notification(cloudService, radioUuid)
                .withPriority(UPriority.UPRIORITY_CS2)
                .withTtl(1000)
                .withPermissionLevel(4211)
                .withToken("SomeToken")
                .withTraceparent("someTraceParent")
                .build(new UPayload(ByteString.copyFrom("Hello World", Charset.defaultCharset()), UPayloadFormat.UPAYLOAD_FORMAT_TEXT));

        mqttClientOfCloud.send(message);

        //should be received by radio
        ArgumentCaptor<UMessage> captor = ArgumentCaptor.captor();
        verify(radioListener, Mockito.timeout(1000).times(1)).onReceive(captor.capture());

        UMessage receivedMessage = captor.getValue();

        assertThat(receivedMessage.getPayload().toString(Charset.defaultCharset())).isEqualTo("Hello World");
        assertThat(receivedMessage.getAttributes().getType()).isEqualTo(UMessageType.UMESSAGE_TYPE_NOTIFICATION);
        assertThat(receivedMessage.getAttributes().getSource().getAuthorityName()).isEqualTo("cloud");
        assertThat(receivedMessage.getAttributes().getSink().getAuthorityName()).isEqualTo("radio");
        assertThat(receivedMessage.getAttributes().getPriority()).isEqualTo(UPriority.UPRIORITY_CS2);
        assertThat(receivedMessage.getAttributes().getTtl()).isEqualTo(1000);
        assertThat(receivedMessage.getAttributes().getPermissionLevel()).isEqualTo(4211);
        assertThat(receivedMessage.getAttributes().getCommstatus()).isEqualTo(UCode.OK);
        assertThat(receivedMessage.getAttributes().getToken()).isEqualTo("SomeToken");
        assertThat(receivedMessage.getAttributes().getTraceparent()).isEqualTo("someTraceParent");
        assertThat(receivedMessage.getAttributes().getPayloadFormat()).isEqualTo(UPayloadFormat.UPAYLOAD_FORMAT_TEXT);
    }
}