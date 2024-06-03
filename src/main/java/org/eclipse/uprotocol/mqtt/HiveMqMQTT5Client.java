package org.eclipse.uprotocol.mqtt;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UStatus;
import org.eclipse.uprotocol.v1.UUri;

class HiveMqMQTT5Client implements UTransport {

    private final Mqtt5AsyncClient client;

    public HiveMqMQTT5Client(Mqtt5Client client) {
        this.client = client.toAsync();
    }

    @Override
    public UStatus send(UMessage uMessage) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public UStatus registerListener(UUri uUri, UListener uListener) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public UStatus unregisterListener(UUri uUri, UListener uListener) {
        throw new RuntimeException("Not implemented");
    }
}
