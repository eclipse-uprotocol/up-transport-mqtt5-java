package org.eclipse.uprotocol.mqtt;

import com.google.protobuf.ByteString;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.v1.UAttributes;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UStatus;
import org.eclipse.uprotocol.v1.UUri;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.eclipse.uprotocol.v1.UCode.INTERNAL;
import static org.eclipse.uprotocol.v1.UCode.OK;

class HiveMqMQTT5Client implements UTransport {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMqMQTT5Client.class);
    private final Mqtt5AsyncClient client;
    private final UUri source;

    public HiveMqMQTT5Client(UUri source, Mqtt5Client client) {
        this.client = client.toAsync();
        this.source = source;
    }

    public CompletableFuture<UStatus> sendAsync(UMessage uMessage) {
        LOG.trace("should send a message:\n{}", uMessage);
        CompletableFuture<UStatus> result = new CompletableFuture<>();

        Mqtt5UserProperties userProperties = Mqtt5UserProperties.builder()
                .add("0", "1")
                .add("1", uMessage.getAttributes().getId().toString())
                .add("2", Integer.toString(uMessage.getAttributes().getTypeValue()))
                .add("3", uMessage.getAttributes().getSource().getAuthorityName())
                .add("4", uMessage.getAttributes().getSink().getAuthorityName())
                .add("5", Integer.toString(uMessage.getAttributes().getPriorityValue()))
                .add("6", Integer.toString(uMessage.getAttributes().getTtl()))
                .add("7", Integer.toString(uMessage.getAttributes().getPermissionLevel()))
                .add("8", Integer.toString(uMessage.getAttributes().getCommstatusValue()))
                .add("9", uMessage.getAttributes().getReqid().toString())
                .add("10", uMessage.getAttributes().getToken())
                .add("11", uMessage.getAttributes().getTraceparent())
                .add("12", Integer.toString(uMessage.getAttributes().getPayloadFormatValue()))
                .build();

        Mqtt5PublishBuilder.Send.Complete<CompletableFuture<Mqtt5PublishResult>> sendHandle = client.publishWith()
                .topic(getTopicForSending(uMessage.getAttributes().getSource(), uMessage.getAttributes().getSink()))
                .payload(uMessage.getPayload().toByteArray())
                .userProperties(userProperties);

        if (uMessage.getAttributes().hasTtl()) {
            sendHandle = sendHandle.messageExpiryInterval(Math.round(uMessage.getAttributes().getTtl() / 1000d));
        }

        sendHandle
                .send()
                .whenCompleteAsync((mqtt5PublishResult, throwable) -> {
                    LOG.trace("got complete callback");
                    if (throwable != null) {
                        LOG.error("Error sending message", throwable);
                        result.complete(UStatus.newBuilder().setCode(INTERNAL).build());
                    } else {
                        LOG.trace("publishResult sending message {}", mqtt5PublishResult);
                        result.complete(UStatus.newBuilder().setCode(OK).build());
                    }
                });


        return result;
    }

    @Override
    public UStatus send(UMessage uMessage) {
        return sendAsync(uMessage).join();
    }

    public CompletableFuture<UStatus> registerListenerAsync(UUri sourceFilter, UUri sinkFilter, UListener listener) {
        LOG.trace("registering Listener for \nsource={}\nsink={}\nlistener={}", sourceFilter, sinkFilter, listener);
        CompletableFuture<UStatus> result = new CompletableFuture<>();

        client.subscribeWith()
                .topicFilter(getTopicFilterForReceiving(sourceFilter, sinkFilter))
                .userProperties()
                .add("listenerId", String.valueOf(listener.hashCode()))
                .applyUserProperties()
                .callback(mqtt5Publish ->
                        listener.onReceive(UMessage.newBuilder()
                                // todo: build UMessage
                                .setAttributes(UAttributes.newBuilder()
//                                                .setSink()
//                                                .setSource()
                                        .build())
                                .setPayload(ByteString.copyFrom(mqtt5Publish.getPayloadAsBytes()))
                                .build()))
                .send()
                .whenCompleteAsync((mqtt5SubAck, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error subscribing to topic", throwable);
                        result.complete(UStatus.newBuilder().setCode(INTERNAL).build());
                    } else {
                        LOG.trace("subscribeResult is {}", mqtt5SubAck);
                        result.complete(UStatus.newBuilder().setCode(OK).build());
                    }
                });

        return result;
    }

    @Override
    public UStatus registerListener(UUri sourceFilter, UUri sinkFilter, UListener listener) {
        return registerListenerAsync(sourceFilter, sinkFilter, listener).join();
    }

    public CompletableFuture<UStatus> unregisterListenerAsync(UUri sourceFilter, UUri sinkFilter, UListener listener) {
        LOG.trace("unregistering Listener for \nsource={}\nsink={}\nlistener={}", sourceFilter, sinkFilter, listener);

        CompletableFuture<UStatus> result = new CompletableFuture<>();

        client.unsubscribeWith()
                .topicFilter(getTopicFilterForReceiving(sourceFilter, sinkFilter))
                .userProperties()
                .add("listenerId", String.valueOf(listener.hashCode()))
                .applyUserProperties()
                .send()
                .whenCompleteAsync((mqtt5UnsubAck, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error subscribing to topic", throwable);
                        result.complete(UStatus.newBuilder().setCode(INTERNAL).build());
                    } else {
                        result.complete(UStatus.newBuilder().setCode(OK).build());
                    }
                });

        return result;
    }

    @Override
    public UStatus unregisterListener(UUri sourceFilter, UUri sinkFilter, UListener listener) {
        return unregisterListenerAsync(sourceFilter, sinkFilter, listener).join();
    }

    @Override
    public UUri getSource() {
        return source;
    }

    private @NotNull MqttTopic getTopicForSending(@NotNull UUri source, @Nullable UUri sink) {
        return MqttTopic.builder()
                .addLevel(String.valueOf(determinateClientIdentifierFromSource(source)))

                .addLevel(source.getAuthorityName())
                .addLevel(String.format("%04x", source.getUeId()))
                .addLevel(String.format("%02x", source.getUeVersionMajor()))
                .addLevel(String.format("%04x", source.getResourceId()))

                .addLevel(Optional.ofNullable(sink).map(UUri::getAuthorityName).orElse(""))
                .addLevel(Optional.ofNullable(sink).map(UUri::getUeId).map(ueId -> String.format("%04x", ueId)).orElse(""))
                .addLevel(Optional.ofNullable(sink).map(UUri::getUeVersionMajor).map(majorVersion -> String.format("%02x", majorVersion)).orElse(""))
                .addLevel(Optional.ofNullable(sink).map(UUri::getResourceId).map(resourceId -> String.format("%04x", resourceId)).orElse(""))

                .build();
    }

    private @NotNull MqttTopicFilter getTopicFilterForReceiving(@Nullable UUri source, @Nullable UUri sink) {
        String singleLevelWildcardAsString = String.valueOf(MqttTopicFilter.SINGLE_LEVEL_WILDCARD);
        return MqttTopicFilter.builder()
                .addLevel(String.valueOf(determinateClientIdentifierFromSource(source)))

                //if source is null or predefined wildcard -> choose singleLevelWildcardAsString, otherwise choose value (the code is the other way around)
                .addLevel(Optional.ofNullable(source).filter(_source -> !"*".equals(_source.getAuthorityName())).map(UUri::getAuthorityName).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getUeId() != 0xffff).map(existingSource -> String.format("%04x", existingSource.getUeId())).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getUeVersionMajor() != 0xff).map(UUri::getUeVersionMajor).map(Object::toString).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(source).filter(_source -> _source.getResourceId() != 0xffff).map(UUri::getResourceId).map(i -> String.format("%04x", i)).orElse(singleLevelWildcardAsString))

                //if sink is null or predefined wildcard -> choose singleLevelWildcardAsString, otherwise choose value (the code is the other way around)
                .addLevel(Optional.ofNullable(sink).filter(_sink -> !"*".equals(_sink.getAuthorityName())).map(UUri::getAuthorityName).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(sink).filter(_sink -> _sink.getUeId() != 0xffff).map(existingSource -> String.format("%04x", existingSource.getUeId())).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(sink).filter(_sink -> _sink.getUeVersionMajor() != 0xff).map(UUri::getUeVersionMajor).map(Object::toString).orElse(singleLevelWildcardAsString))
                .addLevel(Optional.ofNullable(sink).filter(_sink -> _sink.getResourceId() != 0xffff).map(UUri::getResourceId).map(i -> String.format("%04x", i)).orElse(singleLevelWildcardAsString))

                .build();
    }

    private char determinateClientIdentifierFromSource(UUri source) {
        if (source == null) {
            return MqttTopicFilter.SINGLE_LEVEL_WILDCARD;
        }
        if (source.getAuthorityName().equals("cloud")) { //Todo: fix determination of a cloud environment
            return 'c';
        }
        return 'd';
    }
}
