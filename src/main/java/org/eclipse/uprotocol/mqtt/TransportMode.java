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

import java.util.Optional;

import org.eclipse.uprotocol.uri.validator.UriValidator;
import org.eclipse.uprotocol.v1.UUri;

import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;

public enum TransportMode {
    IN_VEHICLE,
    OFF_VEHICLE;

    private static final String MQTT_TOPIC_ANY_SEGMENT_WILDCARD = String.valueOf(MqttTopicFilter.SINGLE_LEVEL_WILDCARD);

    /**
     * Creates an MQTT topic segment from the authority name of a uProtocol URI.
     * @param uri The uProtocol URI to extract the authority name from.
     * @param fallbackAuthority The fallback authority name to use if the URI does not contain one.
     * @return The MQTT topic segment.
     */
    // [impl->dsn~up-transport-mqtt5-d2d-topic-names~1]
    String uriToAuthorityTopicSegment(UUri uri, String fallbackAuthority) {
        return Optional.ofNullable(uri.getAuthorityName())
                .filter(name -> !name.isEmpty())
                .map(name -> {
                    if (UriValidator.hasWildcardAuthority(uri)) {
                        return MQTT_TOPIC_ANY_SEGMENT_WILDCARD;
                    } else {
                        return name;
                    }
                })
                .orElse(fallbackAuthority);
    }

    /**
     * Converts a uProtocol URI to an MQTT topic.
     *
     * @param uri The URI to convert.
     * @param fallbackAuthority The authority name to use if the given URI does not contain an authority.
     * @return The MQTT topic.
     */
    // [impl->dsn~up-transport-mqtt5-e2e-topic-names~1]
    String uriToE2eMqttTopic(UUri uri, String fallbackAuthority) {
        String authority = uriToAuthorityTopicSegment(uri, fallbackAuthority);

        String ueTypeId = UriValidator.hasWildcardEntityTypeId(uri) ?
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD : String.format("%X", uri.getUeId() & 0x0000_FFFF);
        
        String ueInstanceId = UriValidator.hasWildcardEntityInstanceId(uri) ?
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD : String.format("%X", (uri.getUeId() & 0xFFFF_0000) >> 16);

        String ueVer = UriValidator.hasWildcardEntityVersion(uri) ?
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD : String.format("%X", uri.getUeVersionMajor());

        String resId = UriValidator.hasWildcardResourceId(uri) ?
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD : String.format("%X", uri.getResourceId());

        return String.format("%s/%s/%s/%s/%s", authority, ueTypeId, ueInstanceId, ueVer, resId);
    }

    /// Creates an MQTT topic for a source and sink uProtocol URI.
    ///
    /// # Arguments
    /// * `source` - Source URI.
    /// * `sink` - Sink URI.
    /// * `fallback_authority` - The authority name to use if any of the URIs do not
    /// contain an authority.
    String toMqttTopic(
        UUri source,
        Optional<UUri> sink,
        String fallbackAuthority
    ) {
        var topic = new StringBuilder();
        switch (this) {
            case IN_VEHICLE:
                // [impl->dsn~up-transport-mqtt5-e2e-topic-names~1]
                topic.append(this.uriToE2eMqttTopic(source, fallbackAuthority));
                sink.ifPresent(s -> {
                    topic.append('/');
                    topic.append(this.uriToE2eMqttTopic(s, fallbackAuthority));
                });
                return topic.toString();
            
            case OFF_VEHICLE:
            default:
                // [impl->dsn~up-transport-mqtt5-d2d-topic-names~1]
                return sink
                    .map(s -> {
                        topic.append(this.uriToAuthorityTopicSegment(source, fallbackAuthority));
                        topic.append('/');
                        topic.append(this.uriToAuthorityTopicSegment(s, fallbackAuthority));
                        return topic.toString();
                    })
                    .orElseThrow(() -> new IllegalArgumentException(
                        "Off-Vehicle transport requires sink URI for creating MQTT topic"));
        }
    }
}
