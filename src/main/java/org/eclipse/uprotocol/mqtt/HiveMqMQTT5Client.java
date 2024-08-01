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
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserPropertiesBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.transport.validate.UAttributesValidator;
import org.eclipse.uprotocol.uri.serializer.UriSerializer;
import org.eclipse.uprotocol.uuid.serializer.UuidSerializer;
import org.eclipse.uprotocol.v1.*;
import org.eclipse.uprotocol.validation.ValidationResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static org.eclipse.uprotocol.v1.UCode.INTERNAL;
import static org.eclipse.uprotocol.v1.UCode.OK;

class HiveMqMQTT5Client implements UTransport {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMqMQTT5Client.class);
    private static final String USER_PROPERTIES_KEY_FOR_ID = "1";
    private static final String USER_PROPERTIES_KEY_FOR_MESSAGE_TYPE = "2";
    private static final String USER_PROPERTIES_KEY_FOR_SOURCE_NAME = "3";
    private static final String USER_PROPERTIES_KEY_FOR_SINK_NAME = "4";
    private static final String USER_PROPERTIES_KEY_FOR_PRIORITY = "5";
    private static final String USER_PROPERTIES_KEY_FOR_TTL = "6";
    private static final String USER_PROPERTIES_KEY_FOR_PERMISSION_LEVEL = "7";
    private static final String USER_PROPERTIES_KEY_FOR_COMMSTATUS = "8";
    private static final String USER_PROPERTIES_KEY_FOR_REQID = "9";
    private static final String USER_PROPERTIES_KEY_FOR_TOKEN = "10";
    private static final String USER_PROPERTIES_KEY_FOR_TRACEPARENT = "11";
    private static final String USER_PROPERTIES_KEY_FOR_PAYLOAD_FORMAT = "12";
    private final Mqtt5AsyncClient client;
    private final UUri source;

    public HiveMqMQTT5Client(UUri source, Mqtt5Client client) {
        this.client = client.toAsync();
        this.source = source;
    }

    @Override
    public CompletionStage<UStatus> send(UMessage uMessage) {
        LOG.trace("should send a message:\n{}", uMessage);
        CompletableFuture<UStatus> result = new CompletableFuture<>();

        UAttributesValidator validator = UAttributesValidator.getValidator(uMessage.getAttributes());
        ValidationResult validationResult = validator.validate(uMessage.getAttributes());
        if (validationResult.isFailure()) {
            throw new IllegalArgumentException("Invalid message attributes: " + validationResult);
        }

        Mqtt5UserProperties userProperties = buildUserProperties(uMessage.getAttributes());

        Mqtt5PublishBuilder.Send.Complete<CompletableFuture<Mqtt5PublishResult>> sendHandle = buildMqttSendHandle(uMessage, userProperties);

        sendHandle
                .send()
                .whenCompleteAsync((mqtt5PublishResult, throwable) -> {
                    LOG.trace("got complete callback");
                    if (throwable != null) {
                        LOG.error("Error sending message", throwable);
                        result.complete(UStatus.newBuilder().setCode(INTERNAL).build());
                    } else {
                        LOG.trace("publishResult sending message {}", mqtt5PublishResult);
                        result.complete(UStatus.newBuilder().setCode(OK).build());
                    }
                });


        return result;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private Mqtt5PublishBuilder.Send.@NotNull Complete<CompletableFuture<Mqtt5PublishResult>> buildMqttSendHandle(UMessage uMessage, Mqtt5UserProperties userProperties) {
        Mqtt5PublishBuilder.Send.Complete<CompletableFuture<Mqtt5PublishResult>> sendHandle = client.publishWith()
                .topic(getTopicForSending(uMessage.getAttributes().getSource(), uMessage.getAttributes().getSink()))
                .payload(uMessage.getPayload().toByteArray())
                .userProperties(userProperties);

        if (uMessage.getAttributes().hasTtl()) {
            sendHandle.messageExpiryInterval(Math.round(uMessage.getAttributes().getTtl() / 1000d));
        }
        return sendHandle;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static @NotNull Mqtt5UserProperties buildUserProperties(UAttributes attributes) {
        Mqtt5UserPropertiesBuilder builder = Mqtt5UserProperties.builder();
        builder.add("0", "1");
        if (attributes.hasId())
            builder.add(USER_PROPERTIES_KEY_FOR_ID, UuidSerializer.serialize(attributes.getId()));

        if(attributes.getType() != UMessageType.UMESSAGE_TYPE_UNSPECIFIED)
            builder.add(USER_PROPERTIES_KEY_FOR_MESSAGE_TYPE, Integer.toString(attributes.getTypeValue()));

        if(attributes.hasSource())
            builder.add(USER_PROPERTIES_KEY_FOR_SOURCE_NAME, UriSerializer.serialize(attributes.getSource()));

        if(attributes.hasSink())
            builder.add(USER_PROPERTIES_KEY_FOR_SINK_NAME, UriSerializer.serialize(attributes.getSink()));

        if(attributes.getPriority() != UPriority.UPRIORITY_UNSPECIFIED)
            builder.add(USER_PROPERTIES_KEY_FOR_PRIORITY, Integer.toString(attributes.getPriorityValue()));

        if (attributes.hasTtl())
            builder.add(USER_PROPERTIES_KEY_FOR_TTL, Integer.toString(attributes.getTtl()));

        if(attributes.hasPermissionLevel())
            builder.add(USER_PROPERTIES_KEY_FOR_PERMISSION_LEVEL, Integer.toString(attributes.getPermissionLevel()));

        if(attributes.hasCommstatus())
            builder.add(USER_PROPERTIES_KEY_FOR_COMMSTATUS, Integer.toString(attributes.getCommstatusValue()));

        if(attributes.hasReqid())
            builder.add(USER_PROPERTIES_KEY_FOR_REQID, UuidSerializer.serialize(attributes.getReqid()));

        if(attributes.hasToken())
            builder.add(USER_PROPERTIES_KEY_FOR_TOKEN, attributes.getToken());

        if(attributes.hasTraceparent())
            builder.add(USER_PROPERTIES_KEY_FOR_TRACEPARENT, attributes.getTraceparent());

        if(attributes.getPayloadFormat() != UPayloadFormat.UPAYLOAD_FORMAT_UNSPECIFIED)
            builder.add(USER_PROPERTIES_KEY_FOR_PAYLOAD_FORMAT, Integer.toString(attributes.getPayloadFormatValue()));

        return builder.build();
    }

    @Override
    public CompletionStage<UStatus> registerListener(UUri sourceFilter, UUri sinkFilter, UListener listener) {
        LOG.trace("registering Listener for \nsource={}\nsink={}\nlistener={}", sourceFilter, sinkFilter, listener);
        CompletableFuture<UStatus> result = new CompletableFuture<>();


        client.subscribeWith()
                .topicFilter(getTopicFilterForReceiving(sourceFilter, sinkFilter))
                .userProperties()
                .add("listenerId", String.valueOf(listener.hashCode()))
                .applyUserProperties()
                .callback(mqtt5Publish -> {
                    LOG.trace("received message {}", mqtt5Publish);
                    listener.onReceive(UMessage.newBuilder()
                            .setAttributes(extractUAttributesFromReceivedMQTTMessage(mqtt5Publish))
                            .setPayload(ByteString.copyFrom(mqtt5Publish.getPayloadAsBytes()))
                            .build());
                })
                .send()
                .whenCompleteAsync((mqtt5SubAck, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error subscribing to topic", throwable);
                        result.complete(UStatus.newBuilder().setCode(INTERNAL).build());
                    } else {
                        LOG.trace("subscribeResult is {}", mqtt5SubAck);
                        result.complete(UStatus.newBuilder().setCode(OK).build());
                    }
                });

        return result;
    }

    @Override
    public CompletionStage<UStatus> unregisterListener(UUri sourceFilter, UUri sinkFilter, UListener listener) {
        LOG.trace("unregistering Listener for \nsource={}\nsink={}\nlistener={}", sourceFilter, sinkFilter, listener);

        CompletableFuture<UStatus> result = new CompletableFuture<>();

        client.unsubscribeWith()
                .topicFilter(getTopicFilterForReceiving(sourceFilter, sinkFilter))
                .userProperties()
                .add("listenerId", String.valueOf(listener.hashCode()))
                .applyUserProperties()
                .send()
                .whenCompleteAsync((mqtt5UnsubAck, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error subscribing to topic", throwable);
                        result.complete(UStatus.newBuilder().setCode(INTERNAL).build());
                    } else {
                        result.complete(UStatus.newBuilder().setCode(OK).build());
                    }
                });

        return result;
    }

    @Override
    public UUri getSource() {
        return source;
    }

    @Override
    public void close() {
        client.disconnect();
    }

    private @NotNull MqttTopic getTopicForSending(@NotNull UUri source, @Nullable UUri sink) {
        return MqttTopic.builder()
                .addLevel(String.valueOf(determinateClientIdentifierFromSource(source)))

                .addLevel(source.getAuthorityName())
                .addLevel(String.format("%04x", source.getUeId()))
                .addLevel(String.format("%02x", source.getUeVersionMajor()))
                .addLevel(String.format("%04x", source.getResourceId()))

                .addLevel(Optional.ofNullable(sink).map(UUri::getAuthorityName).orElse(""))
                .addLevel(Optional.ofNullable(sink).map(UUri::getUeId).map(ueId -> String.format("%04x", ueId)).orElse(""))
                .addLevel(Optional.ofNullable(sink).map(UUri::getUeVersionMajor).map(majorVersion -> String.format("%02x", majorVersion)).orElse(""))
                .addLevel(Optional.ofNullable(sink).map(UUri::getResourceId).map(resourceId -> String.format("%04x", resourceId)).orElse(""))

                .build();
    }

    private @NotNull MqttTopicFilter getTopicFilterForReceiving(@Nullable UUri source, @Nullable UUri sink) {
        String singleLevelWildcardAsString = String.valueOf(MqttTopicFilter.SINGLE_LEVEL_WILDCARD);
        return MqttTopicFilter.builder()
                .addLevel(String.valueOf(determinateClientIdentifierFromSource(source)))

                //if source is null or predefined wildcard -> choose singleLevelWildcardAsString, otherwise choose value (the code is the other way around)
                .addLevel(Optional.ofNullable(source).filter(_source -> !"*".equals(_source.getAuthorityName())).map(UUri::getAuthorityName).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getUeId() != 0xffff).map(existingSource -> String.format("%04x", existingSource.getUeId())).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getUeVersionMajor() != 0xff).map(UUri::getUeVersionMajor).map(Object::toString).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getResourceId() != 0xffff).map(UUri::getResourceId).map(i -> String.format("%04x", i)).orElse(singleLevelWildcardAsString))

                //if sink is null or predefined wildcard -> choose singleLevelWildcardAsString, otherwise choose value (the code is the other way around)
                .addLevel(Optional.ofNullable(sink).filter(_sink -> !"*".equals(_sink.getAuthorityName())).map(UUri::getAuthorityName).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(sink).filter(_sink -> _sink.getUeId() != 0xffff).map(existingSource -> String.format("%04x", existingSource.getUeId())).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(sink).filter(_sink -> _sink.getUeVersionMajor() != 0xff).map(UUri::getUeVersionMajor).map(Object::toString).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(sink).filter(_sink -> _sink.getResourceId() != 0xffff).map(UUri::getResourceId).map(i -> String.format("%04x", i)).orElse(singleLevelWildcardAsString))

                .build();
    }

    private char determinateClientIdentifierFromSource(UUri source) {
        if (source == null) {
            return MqttTopicFilter.SINGLE_LEVEL_WILDCARD;
        }
        if (source.getAuthorityName().equals("cloud")) { //Todo: fix determination of a cloud environment
            return 'c';
        }
        return 'd';
    }

    private UAttributes extractUAttributesFromReceivedMQTTMessage(@NotNull Mqtt5Publish mqtt5Publish) {
        if (mqtt5Publish.getTopic().getLevels().size() != 9)
            throw new IllegalArgumentException("Topic did not match uProtocol pattern for mqtt messages of this spec");

        Map<String, String> userProperties = convertUserPropertiesToMap(mqtt5Publish.getUserProperties());
        UAttributes.Builder builder = UAttributes.newBuilder();

        userProperties.forEach((key, value) -> {
            Optional<Integer> valueAsInt = Optional.empty();
            try {
                valueAsInt = Optional.of(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                LOG.trace("value is not a number {}", value);
            }
            switch (key) {
                case USER_PROPERTIES_KEY_FOR_ID -> builder.setId(UuidSerializer.deserialize(value));
                case USER_PROPERTIES_KEY_FOR_MESSAGE_TYPE ->
                        valueAsInt.map(UMessageType::forNumber).ifPresent(builder::setType);
                case USER_PROPERTIES_KEY_FOR_SOURCE_NAME -> builder.setSource(UriSerializer.deserialize(value));
                case USER_PROPERTIES_KEY_FOR_SINK_NAME ->  builder.setSink(UriSerializer.deserialize(value));
                case USER_PROPERTIES_KEY_FOR_PRIORITY ->
                        valueAsInt.map(UPriority::forNumber).ifPresent(builder::setPriority);
                case USER_PROPERTIES_KEY_FOR_TTL ->
                        valueAsInt.ifPresent(builder::setTtl);
                case USER_PROPERTIES_KEY_FOR_PERMISSION_LEVEL ->
                        valueAsInt.ifPresent(builder::setPermissionLevel);
                case USER_PROPERTIES_KEY_FOR_COMMSTATUS ->
                        valueAsInt.map(UCode::forNumber).ifPresent(builder::setCommstatus);
                case USER_PROPERTIES_KEY_FOR_REQID -> builder.setReqid(UuidSerializer.deserialize(value));
                case USER_PROPERTIES_KEY_FOR_TOKEN ->
                        builder.setToken(value);
                case USER_PROPERTIES_KEY_FOR_TRACEPARENT ->
                        builder.setTraceparent(value);
                case USER_PROPERTIES_KEY_FOR_PAYLOAD_FORMAT ->
                        valueAsInt.map(UPayloadFormat::forNumber).ifPresent(builder::setPayloadFormat);
                default -> LOG.warn("unknown user properties for key {}", key);
            }
        });

        return builder.build();
    }


    private Map<String, String> convertUserPropertiesToMap(Mqtt5UserProperties userProperties) {
        return userProperties.asList().stream().collect(
                Collectors.toMap(
                        property -> property.getName().toString(),
                        property -> property.getValue().toString())
        );
    }
}
