/**
 * SPDX-FileCopyrightText: 2025 Contributors to the Eclipse Foundation
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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.uprotocol.Uoptions;
import org.eclipse.uprotocol.transport.validator.UAttributesValidator;
import org.eclipse.uprotocol.uri.serializer.UriSerializer;
import org.eclipse.uprotocol.uuid.serializer.UuidSerializer;
import org.eclipse.uprotocol.v1.UAttributes;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UMessageType;
import org.eclipse.uprotocol.v1.UPayloadFormat;
import org.eclipse.uprotocol.v1.UPriority;
import org.eclipse.uprotocol.v1.UUID;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserPropertiesBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

final class Mapping {

    static final String USER_PROPERTIES_KEY_FOR_UPROTOCOL_VERSION = "uP";
    static final String USER_PROPERTIES_KEY_FOR_ID = "1";
    static final String USER_PROPERTIES_KEY_FOR_MESSAGE_TYPE = "2";
    static final String USER_PROPERTIES_KEY_FOR_SOURCE_NAME = "3";
    static final String USER_PROPERTIES_KEY_FOR_SINK_NAME = "4";
    static final String USER_PROPERTIES_KEY_FOR_PRIORITY = "5";
    static final String USER_PROPERTIES_KEY_FOR_TTL = "6";
    static final String USER_PROPERTIES_KEY_FOR_PERMISSION_LEVEL = "7";
    static final String USER_PROPERTIES_KEY_FOR_COMMSTATUS = "8";
    static final String USER_PROPERTIES_KEY_FOR_REQID = "9";
    static final String USER_PROPERTIES_KEY_FOR_TOKEN = "10";
    static final String USER_PROPERTIES_KEY_FOR_TRACEPARENT = "11";
    static final String USER_PROPERTIES_KEY_FOR_PAYLOAD_FORMAT = "12";

    static final int CURRENT_UPROTOCOL_MAJOR_VERSION = 1;

    private static final Logger LOG = LoggerFactory.getLogger(Mapping.class);

    private Mapping() {
        // utility class
    }

    static ByteBuffer getCorrelationData(UUID reqId) {
        var buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(reqId.getMsb());
        buf.putLong(reqId.getLsb());
        buf.flip();
        return buf;
    }

    static @NotNull Optional<ByteBuffer> getCorrelationData(@NotNull UMessage uMessage) {
        if (uMessage.getAttributes().getType() != UMessageType.UMESSAGE_TYPE_RESPONSE
            || !uMessage.getAttributes().hasReqid()) {
            return Optional.empty();
        }
        var reqId = uMessage.getAttributes().getReqid();
        return Optional.of(getCorrelationData(reqId));
    }

    static @NotNull Mqtt5Publish toMqtt5Publish(
            @NotNull UMessage uMessage,
            @NotNull TransportMode mode,
            @NotNull String fallbackAuthority) {
        final var topic = mode.toMqttTopic(
                uMessage.getAttributes().getSource(),
                uMessage.getAttributes().hasSink() ? Optional.of(uMessage.getAttributes().getSink()) : Optional.empty(),
                fallbackAuthority);

        var builder = Mqtt5Publish.builder().topic(topic).qos(MqttQos.AT_LEAST_ONCE);
        builder.userProperties(buildUserProperties(uMessage.getAttributes()));
        if (uMessage.hasPayload()) {
            builder.payload(uMessage.getPayload().toByteArray());
        }

        if (uMessage.getAttributes().hasTtl()) {
            builder.messageExpiryInterval(Math.round(uMessage.getAttributes().getTtl() / 1000d));
        }
        getCorrelationData(uMessage).ifPresent(builder::correlationData);
        
        return builder.build();
    }

    static @NotNull Mqtt5UserProperties buildUserProperties(@NotNull UAttributes attributes) {
        // we assume the properties to represent a valid UMessage
        // so no sanity checks are done here anymore
        Mqtt5UserPropertiesBuilder builder = Mqtt5UserProperties.builder();
        builder.add(USER_PROPERTIES_KEY_FOR_UPROTOCOL_VERSION, Integer.toString(CURRENT_UPROTOCOL_MAJOR_VERSION));

        // add message ID
        builder.add(USER_PROPERTIES_KEY_FOR_ID, UuidSerializer.serialize(attributes.getId()));

        // add message type
        builder.add(
                USER_PROPERTIES_KEY_FOR_MESSAGE_TYPE,
                attributes.getType().getValueDescriptor().getOptions().getExtension(Uoptions.ceName));

        // add message source
        builder.add(USER_PROPERTIES_KEY_FOR_SOURCE_NAME, UriSerializer.serialize(attributes.getSource()));

        if (attributes.hasSink()) {
            builder.add(USER_PROPERTIES_KEY_FOR_SINK_NAME, UriSerializer.serialize(attributes.getSink()));
        }

        if (attributes.getPriority() != UPriority.UPRIORITY_UNSPECIFIED) {
            builder.add(
                    USER_PROPERTIES_KEY_FOR_PRIORITY,
                    attributes.getPriority().getValueDescriptor().getOptions().getExtension(Uoptions.ceName));
        }

        if (attributes.hasTtl()) {
            var ttl = attributes.getTtl();
            if (ttl % 1000 != 0) {
                builder.add(USER_PROPERTIES_KEY_FOR_TTL, Integer.toString(ttl));
            }
        }

        if (attributes.hasPermissionLevel()) {
            builder.add(USER_PROPERTIES_KEY_FOR_PERMISSION_LEVEL, Integer.toString(attributes.getPermissionLevel()));
        }

        if (attributes.hasCommstatus()) {
            builder.add(USER_PROPERTIES_KEY_FOR_COMMSTATUS, Integer.toString(attributes.getCommstatusValue()));
        }

        if (attributes.hasToken()) {
            builder.add(USER_PROPERTIES_KEY_FOR_TOKEN, attributes.getToken());
        }

        if (attributes.hasTraceparent()) {
            builder.add(USER_PROPERTIES_KEY_FOR_TRACEPARENT, attributes.getTraceparent());
        }

        if (attributes.getPayloadFormat() != UPayloadFormat.UPAYLOAD_FORMAT_UNSPECIFIED) {
            builder.add(USER_PROPERTIES_KEY_FOR_PAYLOAD_FORMAT, Integer.toString(attributes.getPayloadFormatValue()));
        }

        return builder.build();
    }

    static UPriority mapPriorityFromUserProperty(String ceValue) {
        return UPriority.getDescriptor().getValues()
                .stream()
                .filter(v -> ceValue.equals(v.getOptions().getExtension(Uoptions.ceName)))
                .findFirst()
                .map(d -> d.getNumber())
                .map(UPriority::forNumber)
                .orElseThrow(() -> new IllegalArgumentException("Invalid priority: " + ceValue));
    }

    static UMessageType mapMessageTypeFromUserProperty(String ceValue) {
        return UMessageType.getDescriptor().getValues()
                .stream()
                .filter(v -> ceValue.equals(v.getOptions().getExtension(Uoptions.ceName)))
                .findFirst()
                .map(d -> d.getNumber())
                .map(UMessageType::forNumber)
                .orElseThrow(() -> new IllegalArgumentException("Invalid message type: " + ceValue));
    }

    static @NotNull UAttributes extractUAttributesFromReceivedMQTTMessage(@NotNull Mqtt5Publish mqtt5Publish) {
        Map<String, String> userProperties = convertUserPropertiesToMap(mqtt5Publish.getUserProperties());
        UAttributes.Builder builder = UAttributes.newBuilder();
        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_UPROTOCOL_VERSION))
                .filter(v -> Integer.toString(CURRENT_UPROTOCOL_MAJOR_VERSION).equals(v))
                .orElseThrow(() -> new IllegalArgumentException("Invalid or missing uProtocol version"));

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_ID))
                .map(UuidSerializer::deserialize)
                .ifPresent(builder::setId);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_MESSAGE_TYPE))
                .map(Mapping::mapMessageTypeFromUserProperty)
                .ifPresent(builder::setType);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_SOURCE_NAME))
                .map(UriSerializer::deserialize)
                .ifPresent(builder::setSource);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_SINK_NAME))
                .map(UriSerializer::deserialize)
                .ifPresent(builder::setSink);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_PRIORITY))
                .map(Mapping::mapPriorityFromUserProperty)
                .ifPresent(builder::setPriority);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_TTL))
                .map(Integer::parseUnsignedInt)
                .ifPresentOrElse(
                    builder::setTtl,
                    () -> {
                        mqtt5Publish.getMessageExpiryInterval()
                            .ifPresent(i -> {
                                try {
                                    builder.setTtl(Math.toIntExact(Math.multiplyExact(i, 1000)));
                                } catch (ArithmeticException e) {
                                    builder.setTtl(0xFFFF_FFFF);
                                    LOG.debug("TTL value is too large, capping at 0xFFFF_FFFF", e);
                                }
                            });
                    });

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_PERMISSION_LEVEL))
                .map(Integer::parseInt)
                .ifPresent(builder::setPermissionLevel);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_COMMSTATUS))
                .map(Integer::parseInt)
                .ifPresent(builder::setCommstatusValue);

        mqtt5Publish.getCorrelationData()
                .map(buf -> UUID.newBuilder()
                        .setMsb(buf.getLong())
                        .setLsb(buf.getLong())
                        .build())
                .ifPresent(builder::setReqid);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_REQID))
                .map(UuidSerializer::deserialize)
                .ifPresent(builder::setReqid);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_TOKEN))
                .ifPresent(builder::setToken);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_TRACEPARENT))
                .ifPresent(builder::setTraceparent);

        Optional.ofNullable(userProperties.get(USER_PROPERTIES_KEY_FOR_PAYLOAD_FORMAT))
                .map(Integer::parseInt)
                .ifPresent(builder::setPayloadFormatValue);

        final var attribs = builder.build();
        final var validator = UAttributesValidator.getValidator(attribs);
        validator.validate(attribs);

        if (validator.isExpired(attribs)) {
            throw new IllegalArgumentException("Message has expired");
        }
        return attribs;
    }

    static Map<String, String> convertUserPropertiesToMap(Mqtt5UserProperties userProperties) {
        return userProperties.asList().stream().collect(
                Collectors.toMap(
                        property -> property.getName().toString(),
                        property -> property.getValue().toString()));
    }

}
