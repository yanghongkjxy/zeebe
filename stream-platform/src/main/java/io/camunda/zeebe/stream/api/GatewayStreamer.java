/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.stream.api;

import io.camunda.zeebe.stream.api.GatewayStreamer.Metadata;
import io.camunda.zeebe.util.buffer.BufferReader;
import java.util.Optional;
import org.agrona.DirectBuffer;

/**
 * A {@link GatewayStreamer} allows the engine to push data back to a single gateway (any). It keeps
 * track of multiple {@link GatewayStream} instances, each with their own ID. The semantics of the
 * ID, associated with the metadata and payload, are owned by the consumer of the API.
 *
 * <p>NOTE: {@link GatewayStream#push(Object, ErrorHandler)} is a side effect, and should be treated
 * as a post-commit task for consistency. TODO: see if the platform cannot already enforce with its
 * own implementation.
 *
 * <p>NOTE: implementations of the {@link GatewayStream#push(Object, ErrorHandler)} method are
 * likely asynchronous. As such, errors handled via the {@link ErrorHandler} may be executed after
 * the initial call. Callers should be careful with the state they close on in the implementations
 * of their {@link ErrorHandler}.
 *
 * @param <M> associated metadata with a single stream
 * @param <Payload> the payload type that can be pushed to the stream
 */
@FunctionalInterface
public interface GatewayStreamer<M extends Metadata, Payload> {
  static <M extends Metadata, P> GatewayStreamer<M, P> noop() {
    return streamId -> Optional.empty();
  }

  /**
   * Can be used to notify listeners that there are items available to be streamed out for a given
   * stream type.
   *
   * @param streamType the type of the stream which has items available
   */
  default void notifyWorkAvailable(final String streamType) {}

  /** Returns a valid stream for the given ID, or {@link Optional#empty()} if there is none. */
  Optional<GatewayStream<M, Payload>> streamFor(final DirectBuffer streamType);

  /**
   * A {@link GatewayStream} allows consumers to push out {@link Payload} types to a stream with the
   * given {@link #metadata()} associated.
   *
   * <p>NOTE: it's up to consumers of this API to interpret the metadata and its relation to the
   * payload.
   *
   * @param <M> associated metadata with the stream
   * @param <Payload> the payload type that can be pushed to the stream
   */
  interface GatewayStream<M extends Metadata, Payload> {

    /** Returns the stream's metadata */
    M metadata();

    /**
     * Pushes the given payload to the stream. Implementations of this are likely asynchronous; it's
     * recommended that callers ensure that the given payload is immutable, and that the error
     * handler does not close over any shared state.
     *
     * @param payload the data to push to the remote gateway
     * @param errorHandler logic to execute if the data could not be pushed to the underlying stream
     */
    void push(final Payload payload, ErrorHandler<Payload> errorHandler);
  }

  /** Represents associated stream metadata, e.g. job activation properties. */
  interface Metadata extends BufferReader {}

  /**
   * Allows consumers of this API to specify error handling logic when a payload cannot be pushed
   * out.
   *
   * @param <Payload> the payload type
   */
  interface ErrorHandler<Payload> {
    void handleError(final Throwable error, Payload data);
  }
}
