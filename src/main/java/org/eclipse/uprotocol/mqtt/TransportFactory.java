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
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.v1.UUri;

public class TransportFactory {

    public static UTransport createInstance(UUri source, Mqtt5Client client) {
        if (source == null || client == null)
            throw new IllegalArgumentException("source and client must not be null");

        return new HiveMqMQTT5Client(source, client);
    }
}
