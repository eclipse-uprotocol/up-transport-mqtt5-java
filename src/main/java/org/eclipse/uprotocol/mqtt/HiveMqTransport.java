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

import com.google.protobuf.ByteString;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5UnsubAckException;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.Mqtt5Unsubscribe;

import org.eclipse.uprotocol.communication.UStatusException;
import org.eclipse.uprotocol.transport.UListener;
import org.eclipse.uprotocol.transport.UTransport;
import org.eclipse.uprotocol.transport.validator.UAttributesValidator;
import org.eclipse.uprotocol.uri.serializer.UriSerializer;
import org.eclipse.uprotocol.uri.validator.UriValidator;
import org.eclipse.uprotocol.v1.UCode;
import org.eclipse.uprotocol.v1.UMessage;
import org.eclipse.uprotocol.v1.UUri;
import org.eclipse.uprotocol.validation.ValidationException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class HiveMqTransport implements UTransport {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMqTransport.class);

    private final Mqtt5AsyncClient client;
    private final String authorityName;
    private final TransportMode mode;

    HiveMqTransport(
        @NotNull Mqtt5Client client,
        @NotNull String authorityName,
        TransportMode mode) {
        Objects.requireNonNull(client);
        Objects.requireNonNull(authorityName);

        if (!client.getState().isConnected()) {
            throw new IllegalStateException("MQTT client must be connected prior to creating transport instance");
        }

        this.client = client.toAsync();
        this.authorityName = authorityName;
        this.mode = mode;
        if (LOG.isInfoEnabled()) {
            LOG.info("created HiveMqTransport [clientId: {}, authorityName: {}, mode: {}]",
                    client.getConfig().getClientIdentifier().map(Object::toString).orElse("<none>"),
                    authorityName,
                    mode);
        }
    }

    UStatusException mapError(Throwable error, String defaultMessage) {
        if (error instanceof Mqtt5PubAckException pubAckEx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("PubAck [{}]", pubAckEx.getMqttMessage());
            }
            switch (pubAckEx.getMqttMessage().getReasonCode()) {
                case NOT_AUTHORIZED:
                    return new UStatusException(UCode.PERMISSION_DENIED, "not authorized", error);
                case QUOTA_EXCEEDED:
                    return new UStatusException(UCode.RESOURCE_EXHAUSTED, "quota exceeded", error);
                default:
                    // fall through
            }
        } else if (error instanceof Mqtt5SubAckException subAckEx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SubAck [{}]", subAckEx.getMqttMessage());
            }
            for (var rc : subAckEx.getMqttMessage().getReasonCodes()) {
                switch (rc) {
                    case NOT_AUTHORIZED:
                        return new UStatusException(UCode.PERMISSION_DENIED, "not authorized", error);
                    case QUOTA_EXCEEDED:
                        return new UStatusException(UCode.RESOURCE_EXHAUSTED, "quota exceeded", error);
                    default:
                        continue;
                }
            }
        } else if (error instanceof Mqtt5UnsubAckException unsubAckEx) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("UnsubAck [{}]", unsubAckEx.getMqttMessage());
            }
            for (var rc : unsubAckEx.getMqttMessage().getReasonCodes()) {
                switch (rc) {
                    case NOT_AUTHORIZED:
                        return new UStatusException(UCode.PERMISSION_DENIED, "not authorized", error);
                    case NO_SUBSCRIPTIONS_EXISTED:
                        return new UStatusException(UCode.FAILED_PRECONDITION, "no subscriptions existed", error);
                    default:
                        continue;
                }
            }
        }
        return new UStatusException(UCode.INTERNAL, defaultMessage, error);
    }

    @Override
    public @NotNull CompletionStage<Void> send(@NotNull UMessage uMessage) {
        Objects.requireNonNull(uMessage);
        try {
            UAttributesValidator.getValidator(uMessage.getAttributes()).validate(uMessage.getAttributes());
        } catch (ValidationException e) {
            var reason = new UStatusException(UCode.INVALID_ARGUMENT, "invalid message attributes", e);
            return CompletableFuture.failedFuture(reason);
        }

        final var mqttPublish = Mapping.toMqtt5Publish(uMessage, this.mode, this.authorityName);
        return client.publish(mqttPublish)
            .thenApply(pubAck -> {
                LOG.trace("publishResult is {}", pubAck);
                return (Void) null;
            })
            .exceptionallyCompose(t -> {
                LOG.debug("Error sending message", t);
                return CompletableFuture.failedStage(
                    mapError(t, "failed to send message"));
            });
    }

    @Override
    public @NotNull CompletionStage<Void> registerListener(
            @NotNull UUri sourceFilter,
            @NotNull Optional<UUri> sinkFilter,
            @NotNull UListener listener) {

        Objects.requireNonNull(sourceFilter);
        Objects.requireNonNull(sinkFilter);
        Objects.requireNonNull(listener);
        try {
            UriValidator.verifyFilterCriteria(sourceFilter, sinkFilter);
        } catch (UStatusException e) {
            return CompletableFuture.failedFuture(e);
        }

        if (LOG.isTraceEnabled()) {
            var sinkFilterUri = sinkFilter.map(UriSerializer::serialize).orElse("<none>");
            LOG.trace(
                    "registering Listener [source: {}, sink: {}, listener: {}]",
                    UriSerializer.serialize(sourceFilter), sinkFilterUri, listener);
        }

        var topicFilter = this.mode.toMqttTopic(sourceFilter, sinkFilter, this.authorityName);
        var subscribe = Mqtt5Subscribe.builder()
            .topicFilter(topicFilter)
            .qos(MqttQos.AT_LEAST_ONCE)
            .noLocal(true)
            .userProperties()
                .add("listenerId", String.valueOf(listener.hashCode()))
                .applyUserProperties()
            .build();

        return client.subscribe(subscribe, mqtt5Publish -> {
            if (LOG.isTraceEnabled()) {
                LOG.trace("received message {}", mqtt5Publish);
            }
            try {
                var attributes = Mapping.extractUAttributesFromReceivedMQTTMessage(mqtt5Publish);
                var msgBuilder = UMessage.newBuilder().setAttributes(attributes);
                mqtt5Publish.getPayload().ifPresent(payload -> {
                    msgBuilder.setPayload(ByteString.copyFrom(payload));
                });
                listener.onReceive(msgBuilder.build());
            } catch (RuntimeException e) {
                LOG.debug("Received invalid uProtocol message, ignoring", e);
            }
        })
        .thenApply(subAck -> (Void) null)
        .exceptionallyCompose(t -> {
            LOG.debug("Error registering listener", t);
            return CompletableFuture.failedStage(
                mapError(t, "failed to register listener for incoming messages"));
        });
    }

    @Override
    public @NotNull CompletionStage<Void> unregisterListener(
            @NotNull UUri sourceFilter,
            @NotNull Optional<UUri> sinkFilter,
            @NotNull UListener listener) {

        Objects.requireNonNull(sourceFilter);
        Objects.requireNonNull(sinkFilter);
        Objects.requireNonNull(listener);
        try {
            UriValidator.verifyFilterCriteria(sourceFilter, sinkFilter);
        } catch (UStatusException e) {
            return CompletableFuture.failedFuture(e);
        }

        if (LOG.isTraceEnabled()) {
            var sinkFilterUri = sinkFilter.map(UriSerializer::serialize).orElse("<none>");
            LOG.trace(
                    "unregistering Listener [source: {}, sink: {}, listener: {}]",
                    UriSerializer.serialize(sourceFilter), sinkFilterUri, listener);
        }

        var topicFilter = this.mode.toMqttTopic(sourceFilter, sinkFilter, this.authorityName);
        var unsubscribe = Mqtt5Unsubscribe.builder()
            .topicFilter(topicFilter)
            .userProperties()
                .add("listenerId", String.valueOf(listener.hashCode()))
                .applyUserProperties()
            .build();

        return client.unsubscribe(unsubscribe)
            .thenApply(subAck -> (Void) null)
            .exceptionallyCompose(t -> {
                LOG.debug("Error unregistering listener", t);
                return CompletableFuture.failedStage(
                    mapError(t, "failed to unregister listener for incoming messages"));
            });
    }
}
