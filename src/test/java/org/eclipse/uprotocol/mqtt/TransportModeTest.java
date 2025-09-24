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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Optional;

import org.eclipse.uprotocol.uri.serializer.UriSerializer;
import org.eclipse.uprotocol.v1.UUri;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TransportModeTest {
    @ParameterizedTest(name = "test mapping of UUri authority [{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
        uri,                             expectedSegment
        up://VIN.vehicles/A8000/2/8A50,  VIN.vehicles
        /A8000/2/8A50,                   localAuthority
        //*/A8000/2/8A50,                +
        """)
    // [utest->dsn~up-transport-mqtt5-d2d-topic-names~1]
    void testUriToAuthorityTopicSegment(String uri, String expectedSegment) {
        var uuri = UriSerializer.deserialize(uri);
        var actualSegment = TransportMode.IN_VEHICLE.uriToAuthorityTopicSegment(
            uuri, "localAuthority");
        assertEquals(actualSegment, expectedSegment);
    }

    @ParameterizedTest(name = "test mapping of UUri [{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
            uri,                             expectedTopic
            up://VIN.vehicles/A8000/2/8A50,  VIN.vehicles/8000/A/2/8A50
            /A8000/2/8A50,                   localAuthority/8000/A/2/8A50
            //*/A8000/2/8A50,                +/8000/A/2/8A50
            //VIN.vehicles/FFFF/2/8A50,      VIN.vehicles/+/0/2/8A50
            //VIN.vehicles/FFFF8000/2/8A50,  VIN.vehicles/8000/+/2/8A50
            //VIN.vehicles/A8000/FF/8A50,    VIN.vehicles/8000/A/+/8A50
            //VIN.vehicles/A8000/2/FFFF,     VIN.vehicles/8000/A/2/+
            """)
    void testUriToE2eMqttTopic(String uri, String expectedTopic) {
        var uuri = UriSerializer.deserialize(uri);
        var topic = TransportMode.IN_VEHICLE.uriToE2eMqttTopic(uuri, "localAuthority");
        assertEquals(expectedTopic, topic);
    }

    @ParameterizedTest(name = "test mapping of source/sink [{index}] {arguments}")
    @CsvSource(useHeadersInDisplayName = true, textBlock = """
        srcUri,                          sinkUri,                        mode,        expectedTopic
        # Publish to a specific topic
        //VIN.vehicles/A8000/2/8A50,        ,                                    IN_VEHICLE,  VIN.vehicles/8000/A/2/8A50
        # Send a notification
        //VIN.vehicles/A8000/2/8A50,        //VIN.vehicles/B8000/3/0,            IN_VEHICLE,  VIN.vehicles/8000/A/2/8A50/VIN.vehicles/8000/B/3/0
        # Send a local RPC request
        /A8000/2/0,                         /B8000/3/1B50,                       IN_VEHICLE,  local_authority/8000/A/2/0/local_authority/8000/B/3/1B50
        # Send an RPC Response
        //VIN.vehicles/B8000/3/1B50,        //VIN.vehicles/A8000/2/0,            IN_VEHICLE,  VIN.vehicles/8000/B/3/1B50/VIN.vehicles/8000/A/2/0
        # Subscribe to incoming RPC requests for a specific method
        //*/FFFFFFFF/FF/FFFF,               /AB34/1/12CD,                        IN_VEHICLE,  +/+/+/+/+/local_authority/AB34/0/1/12CD
        # Subscribe to all incoming messages for uEntities on a given authority in the back end
        //*/FFFFFFFF/FF/FFFF,               //SERVICE.backend/FFFFFFFF/FF/FFFF,  OFF_VEHICLE,  +/SERVICE.backend
        # Subscribe to all messages published to topics of a specific authority
        //other_authority/FFFFFFFF/FF/FFFF, ,                                    IN_VEHICLE,   other_authority/+/+/+/+
        # Streamer subscribes to all inbound messages from the cloud 
        //*/FFFFFFFF/FF/FFFF,               /FFFFFFFF/FF/FFFF,                   OFF_VEHICLE,  +/local_authority
        # Subscribe to all publish messages from devices within the vehicle
        //*/FFFFFFFF/FF/FFFF,               ,                                    IN_VEHICLE,   +/+/+/+/+
        # Subscribe to all message types but publish messages sent from a specific authority
        //other_authority/FFFFFFFF/FF/FFFF, //*/FFFFFFFF/FF/FFFF,                IN_VEHICLE,   other_authority/+/+/+/+/+/+/+/+/+
        """)
    // [utest->dsn~up-transport-mqtt5-e2e-topic-names~1]
    // [utest->dsn~up-transport-mqtt5-d2d-topic-names~1]
    void testToMqttTopicString(
        String srcUri,
        String sinkUri,
        TransportMode mode,
        String expectedTopic
    ) {
        var srcUuri = UriSerializer.deserialize(srcUri);
        Optional<UUri> sinkUUri = sinkUri == null ? Optional.empty() : Optional.of(UriSerializer.deserialize(sinkUri));
        assertEquals(expectedTopic, mode.toMqttTopic(srcUuri, sinkUUri, "local_authority"));
    }

    @Test
    void testToMqttTopicRequiresSinkForOffVehicle() {
        var srcUuri = UriSerializer.deserialize("//VIN.vehicles/A8000/2/8A50");
        assertThrows(
            IllegalArgumentException.class,
            () -> TransportMode.OFF_VEHICLE.toMqttTopic(srcUuri, Optional.empty(), "localAuthority")
        );
    }
}
