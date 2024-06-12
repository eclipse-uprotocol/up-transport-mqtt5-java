package org.eclipse.uprotocol.mqtt;

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.v1.UUri;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

class TransportFactoryTest {


    @Test
    void givenNoClient_whenInvokeCreateInstance_shouldThrowAnException() {
        assertThatCode(() -> TransportFactory.createInstance(mock(UUri.class), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("source and client must not be null");
    }

    @Test
    void givenValidClient_whenInvokeCreateInstance_shouldReturnInstanceOfUTransport() {
        Mqtt5Client mockedClient = mock(Mqtt5Client.class);

        var result = TransportFactory.createInstance(mock(UUri.class), mockedClient);

        assertThat(result)
                .isInstanceOf(HiveMqMQTT5Client.class)
                .isInstanceOf(UTransport.class);
    }
}