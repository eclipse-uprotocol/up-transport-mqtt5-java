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

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import java.util.Objects;

import org.eclipse.uprotocol.transport.UTransport;

public final class HiveMqTransportFactory {

    private HiveMqTransportFactory() {
        // utility class
    }

    public static UTransport createInstance(Mqtt5Client client, TransportMode mode, String authorityName) {
        Objects.requireNonNull(client);
        Objects.requireNonNull(authorityName);
        return new HiveMqTransport(client, authorityName, mode);
    }
}
