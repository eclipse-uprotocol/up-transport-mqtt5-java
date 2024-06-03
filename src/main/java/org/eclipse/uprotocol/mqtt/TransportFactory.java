package org.eclipse.uprotocol.mqtt;

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import org.eclipse.uprotocol.transport.UTransport;

public class TransportFactory {

    public static UTransport createInstance(final Mqtt5Client client) {
        assert client != null : "client must not be null";
        return new HiveMqMQTT5Client(client);
    }
}
