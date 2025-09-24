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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.eclipse.uprotocol.Uoptions;
import org.eclipse.uprotocol.uri.serializer.UriSerializer;
import org.eclipse.uprotocol.uuid.factory.UuidFactory;
import org.eclipse.uprotocol.uuid.serializer.UuidSerializer;
import org.eclipse.uprotocol.v1.UAttributes;
import org.eclipse.uprotocol.v1.UCode;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UMessageType;
import org.eclipse.uprotocol.v1.UPayloadFormat;
import org.eclipse.uprotocol.v1.UPriority;
import org.eclipse.uprotocol.v1.UUID;
import org.eclipse.uprotocol.validation.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

class MappingTest {

    private static final String MSG_TOKEN = "the token";
    private static final String MSG_TRACEPARENT = "traceparent";
    private static final int MSG_PERMISSION_LEVEL = 15;

    private record ExpectedValues(UAttributes attributes, Mqtt5Publish publish) {
        // empty
    }

    // Helper function to construct UAttributes object for testing.
    static UAttributes createAttributes(
        Optional<UMessageType> type,
        Optional<UUID> id,
        Optional<String> source,
        Optional<String> sink,
        Optional<UPriority> priority,
        OptionalInt ttl, // milliseconds
        OptionalInt permLevel,
        Optional<UCode> commstatus,
        Optional<UUID> reqid,
        Optional<String> token,
        Optional<String> traceparent,
        Optional<UPayloadFormat> payloadFormat
    ) {
        var builder = UAttributes.newBuilder();
        type.ifPresent(builder::setType);
        id.ifPresent(builder::setId);
        source.ifPresent(s -> builder.setSource(UriSerializer.deserialize(s)));
        sink.ifPresent(s -> builder.setSink(UriSerializer.deserialize(s)));
        priority.ifPresent(builder::setPriority);
        ttl.ifPresent(v -> builder.setTtl(v > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) v));
        permLevel.ifPresent(builder::setPermissionLevel);
        commstatus.ifPresent(builder::setCommstatus);
        reqid.ifPresent(builder::setReqid);
        token.ifPresent(builder::setToken);
        traceparent.ifPresent(builder::setTraceparent);
        payloadFormat.ifPresent(builder::setPayloadFormat);
        return builder.build();
    }

    // Helper function to create mqtt properties for testing.
    static Mqtt5Publish createMqttPublish(
        OptionalInt majorVersion,
        Optional<UMessageType> type,
        Optional<UUID> id,
        Optional<String> source,
        Optional<String> sink,
        Optional<UPriority> priority,
        OptionalInt ttl, // milliseconds
        OptionalInt permLevel,
        Optional<UCode> commstatus,
        Optional<UUID> reqid,
        Optional<String> token,
        Optional<String> traceparent,
        Optional<UPayloadFormat> payloadFormat
    ) {
        var publishBuilder = Mqtt5Publish.builder().topic("test/topic").qos(MqttQos.AT_LEAST_ONCE);
        var propsBuilder = Mqtt5UserProperties.builder();
        majorVersion.ifPresent(v -> propsBuilder.add(
            Mapping.USER_PROPERTIES_KEY_FOR_UPROTOCOL_VERSION,
            String.valueOf(v)));
        type.ifPresent(t -> propsBuilder.add(
            Mapping.USER_PROPERTIES_KEY_FOR_MESSAGE_TYPE,
            t.getValueDescriptor().getOptions().getExtension(Uoptions.ceName)));
        id.ifPresent(i -> propsBuilder.add(Mapping.USER_PROPERTIES_KEY_FOR_ID, UuidSerializer.serialize(i)));
        source.ifPresent(s -> propsBuilder.add(Mapping.USER_PROPERTIES_KEY_FOR_SOURCE_NAME, s));
        sink.ifPresent(s -> propsBuilder.add(Mapping.USER_PROPERTIES_KEY_FOR_SINK_NAME, s));
        priority.filter(p -> p != UPriority.UPRIORITY_UNSPECIFIED)
            .ifPresent(p -> propsBuilder.add(
                Mapping.USER_PROPERTIES_KEY_FOR_PRIORITY,
                p.getValueDescriptor().getOptions().getExtension(Uoptions.ceName)));
        ttl.ifPresent(t -> {
            publishBuilder.messageExpiryInterval(Math.round(t / 1000d));
            if (t % 1000 > 0) {
                propsBuilder.add(Mapping.USER_PROPERTIES_KEY_FOR_TTL, String.valueOf(t));
            }
        });
        permLevel.ifPresent(p -> propsBuilder.add(Mapping.USER_PROPERTIES_KEY_FOR_PERMISSION_LEVEL, String.valueOf(p)));
        commstatus.ifPresent(c -> propsBuilder.add(
            Mapping.USER_PROPERTIES_KEY_FOR_COMMSTATUS,
            String.valueOf(c.getNumber())));
        reqid.ifPresent(r -> publishBuilder.correlationData(Mapping.getCorrelationData(r)));
        token.ifPresent(t -> propsBuilder.add(Mapping.USER_PROPERTIES_KEY_FOR_TOKEN, t));
        traceparent.ifPresent(t -> propsBuilder.add(Mapping.USER_PROPERTIES_KEY_FOR_TRACEPARENT, t));
        payloadFormat.ifPresent(p -> propsBuilder.add(
            Mapping.USER_PROPERTIES_KEY_FOR_PAYLOAD_FORMAT,
            String.valueOf(p.getNumber())));
        publishBuilder.userProperties(propsBuilder.build());
        return publishBuilder.build();
    }

    // Helper function used to create a UAttributes object and corresponding
    // MQTT properties object for testing and comparison
    static ExpectedValues createTestAttributesAndProperties(
        OptionalInt majorVersion,
        Optional<UMessageType> type,
        Optional<UUID> id,
        Optional<String> source,
        Optional<String> sink,
        Optional<UPriority> priority,
        OptionalInt ttl, // milliseconds
        OptionalInt permLevel,
        Optional<UCode> commstatus,
        Optional<UUID> reqid,
        Optional<String> token,
        Optional<String> traceparent,
        Optional<UPayloadFormat> payloadFormat
    ) {
        var attributes = createAttributes(
            type,
            id,
            source,
            sink,
            priority,
            ttl,
            permLevel,
            commstatus,
            reqid,
            token,
            traceparent,
            payloadFormat
        );
        var publish = createMqttPublish(
            majorVersion,
            type,
            id,
            source,
            sink,
            priority,
            ttl,
            permLevel,
            commstatus,
            reqid,
            token,
            traceparent,
            payloadFormat
        );
        return new ExpectedValues(attributes, publish);
    }

    //
    // MQTT PUBLISH -> UAttributes
    //

    @Test
    void testExtractUAttributesFromMqttPublishUsesMaxTtl() {
        var publishTemp = createMqttPublish(
            OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
            Optional.of(UMessageType.UMESSAGE_TYPE_PUBLISH),
            Optional.of(UuidFactory.create()),
            Optional.of("//VIN.vehicles/A8000/2/8A50"),
            Optional.empty(),
            Optional.empty(),
            OptionalInt.empty(),
            OptionalInt.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        var publish = Mqtt5Publish.builder().topic("test/topic").qos(MqttQos.AT_LEAST_ONCE)
            .userProperties(publishTemp.getUserProperties())
            // set message expiry interval to a value that exceeds 0xFFFF_FFFF when converted to ms
            .messageExpiryInterval(0xFFFF_FFF0L)
            .build();
        var attribs = Mapping.extractUAttributesFromReceivedMQTTMessage(publish);
        assertThat(attribs.getTtl()).isEqualTo(0xFFFF_FFFF);
    }

    static Stream<Arguments> provideExpectedValuesForMappingFromMqtt5Publish() {
        return Stream.of(
            // for valid Publish message
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_PUBLISH),
                    Optional.of(UuidFactory.create()),
                    Optional.of("//VIN.vehicles/A8000/2/8A50"),
                    Optional.empty(),
                    Optional.of(UPriority.UPRIORITY_CS5),
                    OptionalInt.empty(),
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(UPayloadFormat.UPAYLOAD_FORMAT_TEXT)
                ),
                Optional.empty()
            ),
            // for valid Notification message
            // [utest->dsn~up-attributes-priority~1]
            // [utest->dsn~up-attributes-ttl~1]
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_NOTIFICATION),
                    Optional.of(
                        // timestamp: 1000ms since UNIX epoch
                        UUID.newBuilder().setMsb(0x0000000010007000L).setLsb(0x8010101010101a1aL).build()),
                    Optional.of("//VIN.vehicles/A8000/2/8A50"),
                    Optional.of("//VIN.vehicles/B8000/3/0"),
                    Optional.empty(),
                    OptionalInt.of(0), // do not expire
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(MSG_TRACEPARENT),
                    Optional.empty()
                ),
                Optional.empty()
            ),
            // for valid Request message
            // [utest->dsn~up-attributes-ttl-timeout~1]
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_REQUEST),
                    Optional.of(UuidFactory.create()),
                    Optional.of("//VIN.vehicles/A8000/2/0"),
                    Optional.of("//VIN.vehicles/B8000/3/1B50"),
                    Optional.of(UPriority.UPRIORITY_CS4),
                    OptionalInt.of(5400),
                    OptionalInt.of(MSG_PERMISSION_LEVEL),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(MSG_TOKEN),
                    Optional.of(MSG_TRACEPARENT),
                    Optional.of(UPayloadFormat.UPAYLOAD_FORMAT_RAW)
                ),
                Optional.empty()
            ),
            // for valid Response message
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_RESPONSE),
                    Optional.of(UuidFactory.create()),
                    Optional.of("//VIN.vehicles/B8000/3/1B50"),
                    Optional.of("//VIN.vehicles/A8000/2/0"),
                    Optional.of(UPriority.UPRIORITY_CS4),
                    OptionalInt.of(3000),
                    OptionalInt.empty(),
                    Optional.of(UCode.UNIMPLEMENTED),
                    Optional.of(UuidFactory.create()),
                    Optional.empty(),
                    Optional.of(MSG_TRACEPARENT),
                    Optional.empty()
                ),
                Optional.empty()
            ),
            // fails for Publish message with invalid source URI
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_PUBLISH),
                    Optional.of(UuidFactory.create()),
                    // source must have resource ID >= 0x8000
                    Optional.of("//VIN.vehicles/A8000/2/1A50"),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalInt.empty(),
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()
                ),
                Optional.of(ValidationException.class)
            ),
            // fails for Publish message with invalid uProtocol version
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION + 1),
                    Optional.of(UMessageType.UMESSAGE_TYPE_PUBLISH),
                    Optional.of(UuidFactory.create()),
                    Optional.of("//VIN.vehicles/A8000/2/8A50"),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalInt.empty(),
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()
                ),
                Optional.of(IllegalArgumentException.class)
            ),
            // fails for expired Publish message
            // [utest->dsn~up-attributes-ttl-timeout~1]
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_PUBLISH),
                    Optional.of(
                        // timestamp: 1000ms since UNIX epoch
                        UUID.newBuilder().setMsb(0x0000000010007000L).setLsb(0x8010101010101a1aL).build()),
                    Optional.of("//VIN.vehicles/A8000/2/AA50"),
                    Optional.empty(),
                    Optional.empty(),
                    // message expiry interval: 12.5s
                    // i.e. this message has expired decades ago
                    OptionalInt.of(12500),
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()
                ),
                Optional.of(IllegalArgumentException.class)
            )
        );
    }

    @ParameterizedTest(name = "test mapping from MQTT PUBLISH to UAttributes: {index} => {arguments}")
    @MethodSource("provideExpectedValuesForMappingFromMqtt5Publish")
    // [utest->dsn~up-transport-mqtt5-attributes-mapping~1]
    void testCreateUAttributesFromMqttPublish(
            ExpectedValues expectedAttributes,
            Optional<Class<? extends Exception>> expectedException) {
        if (expectedException.isPresent()) {
            assertThrows(
                expectedException.get(),
                () -> Mapping.extractUAttributesFromReceivedMQTTMessage(expectedAttributes.publish));
        } else {
            var attributes = Mapping.extractUAttributesFromReceivedMQTTMessage(expectedAttributes.publish);
            assertThat(attributes).isEqualTo(expectedAttributes.attributes);
        }
    }

    //
    // UAttributes -> MQTT Properties
    //

    static Stream<Arguments> provideExpectedValuesForMappingFromUAttributes() {
        return Stream.of(
            // for Publish message
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_PUBLISH),
                    Optional.of(UuidFactory.create()),
                    Optional.of("/A10D/4/B3AA"),
                    Optional.empty(),
                    Optional.of(UPriority.UPRIORITY_CS5),
                    OptionalInt.empty(),
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(UPayloadFormat.UPAYLOAD_FORMAT_TEXT)
                )
            ),
            // for Notification message
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_NOTIFICATION),
                    Optional.of(UuidFactory.create()),
                    Optional.of("/A10D/4/B3AA"),
                    Optional.of("/103/2/0"),
                    Optional.empty(),
                    OptionalInt.empty(),
                    OptionalInt.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(MSG_TRACEPARENT),
                    Optional.empty()
                )
            ),
            // for RPC Request message
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_REQUEST),
                    Optional.of(UuidFactory.create()),
                    Optional.of("/A10D/4/0"),
                    Optional.of("/103/2/71A3"),
                    Optional.of(UPriority.UPRIORITY_CS4),
                    OptionalInt.of(2100),
                    OptionalInt.of(MSG_PERMISSION_LEVEL),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(MSG_TOKEN),
                    Optional.of(MSG_TRACEPARENT),
                    Optional.of(UPayloadFormat.UPAYLOAD_FORMAT_RAW)
                )
            ),
            // for RPC Response message
            Arguments.of(
                createTestAttributesAndProperties(
                    OptionalInt.of(Mapping.CURRENT_UPROTOCOL_MAJOR_VERSION),
                    Optional.of(UMessageType.UMESSAGE_TYPE_RESPONSE),
                    Optional.of(UuidFactory.create()),
                    Optional.of("/103/2/71A3"),
                    Optional.of("/A10D/4/0"),
                    Optional.of(UPriority.UPRIORITY_CS4),
                    OptionalInt.of(4150),
                    OptionalInt.empty(),
                    Optional.of(UCode.UNIMPLEMENTED),
                    Optional.of(UuidFactory.create()),
                    Optional.empty(),
                    Optional.of(MSG_TRACEPARENT),
                    Optional.empty()
                )
            )
        );
    }

    @ParameterizedTest(name = "test mapping from UAttributes to MQTT PUBLISH: {index} => {arguments}")
    @MethodSource("provideExpectedValuesForMappingFromUAttributes")
    // [utest->dsn~up-transport-mqtt5-attributes-mapping~1]
    void testCreateMqttPublishFromUAttributes(ExpectedValues expectedValues) {
        var message = UMessage.newBuilder()
            .setAttributes(expectedValues.attributes)
            .build();
        var publish = Mapping.toMqtt5Publish(message, TransportMode.IN_VEHICLE, "localAuthority");
        assertMqttPublish(publish, expectedValues.publish);
    }

    /// Verifies that two MQTT PUBLISH packets contain the same data
    void assertMqttPublish(Mqtt5Publish publish, Mqtt5Publish expectedPublish) {
        assertThat(publish.getMessageExpiryInterval()).isEqualTo(expectedPublish.getMessageExpiryInterval());
        assertThat(publish.getCorrelationData()).isEqualTo(expectedPublish.getCorrelationData());
        List<Mqtt5UserProperty> userProps = new ArrayList<>(publish.getUserProperties().asList());
        List<Mqtt5UserProperty> expectedUserProps = new ArrayList<>(expectedPublish.getUserProperties().asList());
        assertThat(userProps).containsAll(expectedUserProps);
    }

    @Test
    void testMapPriorityFromUserProperty() {
        assertEquals(UPriority.UPRIORITY_CS1, Mapping.mapPriorityFromUserProperty("CS1"));
        assertEquals(UPriority.UPRIORITY_CS2, Mapping.mapPriorityFromUserProperty("CS2"));
        assertEquals(UPriority.UPRIORITY_CS3, Mapping.mapPriorityFromUserProperty("CS3"));
        assertEquals(UPriority.UPRIORITY_CS4, Mapping.mapPriorityFromUserProperty("CS4"));
        assertEquals(UPriority.UPRIORITY_CS5, Mapping.mapPriorityFromUserProperty("CS5"));
        assertEquals(UPriority.UPRIORITY_CS6, Mapping.mapPriorityFromUserProperty("CS6"));
        assertThrows(
            IllegalArgumentException.class,
            () -> Mapping.mapPriorityFromUserProperty("invalid"));
    }

    @Test
    void testMapMessageTypeFromUserProperty() {
        assertEquals(
            UMessageType.UMESSAGE_TYPE_PUBLISH,
            Mapping.mapMessageTypeFromUserProperty("up-pub.v1"));
        assertEquals(
                UMessageType.UMESSAGE_TYPE_NOTIFICATION,
                Mapping.mapMessageTypeFromUserProperty("up-not.v1"));
        assertEquals(
                UMessageType.UMESSAGE_TYPE_REQUEST,
                Mapping.mapMessageTypeFromUserProperty("up-req.v1"));
        assertEquals(
                UMessageType.UMESSAGE_TYPE_RESPONSE,
                Mapping.mapMessageTypeFromUserProperty("up-res.v1"));
        assertThrows(
                IllegalArgumentException.class,
                () -> Mapping.mapMessageTypeFromUserProperty("invalid"));
    }
}
