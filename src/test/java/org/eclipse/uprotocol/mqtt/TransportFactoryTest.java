package org.eclipse.uprotocol.mqtt;

import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import org.eclipse.uprotocol.transport.UTransport;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

class TransportFactoryTest {


    @Test
    void whenCalledWithoutClient_shouldThrowAnException() {
        assertThatCode(() -> TransportFactory.createInstance(null))
                .isInstanceOf(AssertionError.class)
                .hasMessage("client must not be null");
    }

    @Test
    void whenCalledWithClient_shouldReturnInstanceOfUTransport() {
        Mqtt5Client mockedClient = mock(Mqtt5Client.class);

        var result = TransportFactory.createInstance(mockedClient);

        assertThat(result)
                .isInstanceOf(HiveMqMQTT5Client.class)
                .isInstanceOf(UTransport.class);
    }
}