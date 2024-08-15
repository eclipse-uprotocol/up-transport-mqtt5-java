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
import com.hivemq.client.mqtt.datatypes.MqttTopicBuilder;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilterBuilder;
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
import org.eclipse.uprotocol.uri.factory.UriFactory;
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

    public static final Logger LOG = LoggerFactory.getLogger(HiveMqMQTT5Client.class);
    private final Mqtt5AsyncClient client;
    private final UUri source;

    public HiveMqMQTT5Client(UUri source, Mqtt5Client client) {
        this.client = client.toAsync();
        this.source = source;
    }

    @Override
    public CompletionStage<UStatus> send(UMessage uMessage) {
        LOG.trace("should send a message:\n{}", uMessage);

        UAttributesValidator validator = UAttributesValidator.getValidator(uMessage.getAttributes());
        ValidationResult validationResult = validator.validate(uMessage.getAttributes());
        if (validationResult.isFailure()) {
            throw new IllegalArgumentException("Invalid message attributes: " + validationResult);
        }
        if(uMessage.getAttributes().hasTtl() && uMessage.getAttributes().getTtl() < 500){
            throw new IllegalArgumentException("TimeToLive needs to be at least 500ms. All smaller ttls will be dropped immediately by hiveMq");
        }

        Mqtt5UserProperties userProperties = buildUserProperties(uMessage.getAttributes());

        Mqtt5PublishBuilder.Send.Complete<CompletableFuture<Mqtt5PublishResult>> sendHandle = buildMqttSendHandle(uMessage, userProperties);

        CompletableFuture<UStatus> result = new CompletableFuture<>();
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
                .topic(getTopicForSending(uMessage.getAttributes().getSource(), uMessage.getAttributes().hasSink() ? uMessage.getAttributes().getSink() : null))
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
            builder.add(String.valueOf(UAttributes.ID_FIELD_NUMBER), UuidSerializer.serialize(attributes.getId()));

        if(attributes.getType() != UMessageType.UMESSAGE_TYPE_UNSPECIFIED)
            builder.add(String.valueOf(UAttributes.TYPE_FIELD_NUMBER), Integer.toString(attributes.getTypeValue()));

        if(attributes.hasSource())
            builder.add(String.valueOf(UAttributes.SOURCE_FIELD_NUMBER), UriSerializer.serialize(attributes.getSource()));

        if(attributes.hasSink())
            builder.add(String.valueOf(UAttributes.SINK_FIELD_NUMBER), UriSerializer.serialize(attributes.getSink()));

        if(attributes.getPriority() != UPriority.UPRIORITY_UNSPECIFIED)
            builder.add(String.valueOf(UAttributes.PRIORITY_FIELD_NUMBER), Integer.toString(attributes.getPriorityValue()));

        if (attributes.hasTtl())
            builder.add(String.valueOf(UAttributes.TTL_FIELD_NUMBER), Integer.toString(attributes.getTtl()));

        if(attributes.hasPermissionLevel())
            builder.add(String.valueOf(UAttributes.PERMISSION_LEVEL_FIELD_NUMBER), Integer.toString(attributes.getPermissionLevel()));

        if(attributes.hasCommstatus())
            builder.add(String.valueOf(UAttributes.COMMSTATUS_FIELD_NUMBER), Integer.toString(attributes.getCommstatusValue()));

        if(attributes.hasReqid())
            builder.add(String.valueOf(UAttributes.REQID_FIELD_NUMBER), UuidSerializer.serialize(attributes.getReqid()));

        if(attributes.hasToken())
            builder.add(String.valueOf(UAttributes.TOKEN_FIELD_NUMBER), attributes.getToken());

        if(attributes.hasTraceparent())
            builder.add(String.valueOf(UAttributes.TRACEPARENT_FIELD_NUMBER), attributes.getTraceparent());

        if(attributes.getPayloadFormat() != UPayloadFormat.UPAYLOAD_FORMAT_UNSPECIFIED)
            builder.add(String.valueOf(UAttributes.PAYLOAD_FORMAT_FIELD_NUMBER), Integer.toString(attributes.getPayloadFormatValue()));

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
        MqttTopicBuilder.Complete build = MqttTopic.builder()
                .addLevel(String.valueOf(determinateClientIdentifierFromSource(source)))

                .addLevel(source.getAuthorityName())
                .addLevel(String.format("%04x", source.getUeId()))
                .addLevel(String.format("%02x", source.getUeVersionMajor()))
                .addLevel(String.format("%04x", source.getResourceId()));
        if (sink != null) {
            build = build
                    .addLevel(Optional.of(sink).map(UUri::getAuthorityName).orElse(""))
                    .addLevel(Optional.of(sink).map(UUri::getUeId).map(ueId -> String.format("%04x", ueId)).orElse(""))
                    .addLevel(Optional.of(sink).map(UUri::getUeVersionMajor).map(majorVersion -> String.format("%02x", majorVersion)).orElse(""))
                    .addLevel(Optional.of(sink).map(UUri::getResourceId).map(resourceId -> String.format("%04x", resourceId)).orElse(""));
        } else {
            LOG.trace("no sink defined, therefor sink topics ar e removed (req. since up-spec 1.6.0)");
        }
        return build
                .build();
    }

    private @NotNull MqttTopicFilter getTopicFilterForReceiving(@Nullable UUri source, @Nullable UUri sink) {
        String singleLevelWildcardAsString = String.valueOf(MqttTopicFilter.SINGLE_LEVEL_WILDCARD);
        MqttTopicFilterBuilder.Complete builder = MqttTopicFilter.builder()
                .addLevel(String.valueOf(determinateClientIdentifierFromSource(source)))

                //if source is null or predefined wildcard -> choose singleLevelWildcardAsString, otherwise choose value (the code is the other way around)
                .addLevel(Optional.ofNullable(source).filter(_source -> !"*".equals(_source.getAuthorityName())).map(UUri::getAuthorityName).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getUeId() != 0xffff).map(existingSource -> String.format("%04x", existingSource.getUeId())).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getUeVersionMajor() != 0xff).map(UUri::getUeVersionMajor).map(Object::toString).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getResourceId() != 0xffff).map(UUri::getResourceId).map(i -> String.format("%04x", i)).orElse(singleLevelWildcardAsString));

        if (sink == null || UriFactory.ANY.equals(sink)) {
            builder.addLevel(String.valueOf(MqttTopicFilter.MULTI_LEVEL_WILDCARD));
        } else {
            builder = builder
                    .addLevel(Optional.of(sink).filter(_sink -> !"*".equals(_sink.getAuthorityName())).map(UUri::getAuthorityName).orElse(singleLevelWildcardAsString))
                    .addLevel(Optional.of(sink).filter(_sink -> _sink.getUeId() != 0xffff).map(existingSource -> String.format("%04x", existingSource.getUeId())).orElse(singleLevelWildcardAsString))
                    .addLevel(Optional.of(sink).filter(_sink -> _sink.getUeVersionMajor() != 0xff).map(UUri::getUeVersionMajor).map(Object::toString).orElse(singleLevelWildcardAsString))
                    .addLevel(Optional.of(sink).filter(_sink -> _sink.getResourceId() != 0xffff).map(UUri::getResourceId).map(i -> String.format("%04x", i)).orElse(singleLevelWildcardAsString));
        }
        return builder.build();
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
        if (mqtt5Publish.getTopic().getLevels().size() != 9 && mqtt5Publish.getTopic().getLevels().size() != 5)
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
            int keyAsNumber;
            try {
                keyAsNumber = Integer.parseInt(key);
            } catch (NumberFormatException e) {
                LOG.error("key is not a number {}", key);
                return;
            }
            //@formatter:off
            switch (keyAsNumber) {
                case UAttributes.ID_FIELD_NUMBER -> builder.setId(UuidSerializer.deserialize(value));
                case UAttributes.TYPE_FIELD_NUMBER -> valueAsInt.map(UMessageType::forNumber).ifPresent(builder::setType);
                case UAttributes.SOURCE_FIELD_NUMBER -> builder.setSource(UriSerializer.deserialize(value));
                case UAttributes.SINK_FIELD_NUMBER ->  builder.setSink(UriSerializer.deserialize(value));
                case UAttributes.PRIORITY_FIELD_NUMBER -> valueAsInt.map(UPriority::forNumber).ifPresent(builder::setPriority);
                case UAttributes.TTL_FIELD_NUMBER -> valueAsInt.ifPresent(builder::setTtl);
                case UAttributes.PERMISSION_LEVEL_FIELD_NUMBER -> valueAsInt.ifPresent(builder::setPermissionLevel);
                case UAttributes.COMMSTATUS_FIELD_NUMBER -> valueAsInt.map(UCode::forNumber).ifPresent(builder::setCommstatus);
                case UAttributes.REQID_FIELD_NUMBER -> builder.setReqid(UuidSerializer.deserialize(value));
                case UAttributes.TOKEN_FIELD_NUMBER -> builder.setToken(value);
                case UAttributes.TRACEPARENT_FIELD_NUMBER -> builder.setTraceparent(value);
                case UAttributes.PAYLOAD_FORMAT_FIELD_NUMBER -> valueAsInt.map(UPayloadFormat::forNumber).ifPresent(builder::setPayloadFormat);
                default -> LOG.warn("unknown user properties for key {}", key);
            }
            //@formatter:on
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
