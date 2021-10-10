/*
 * Copyright 2018-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging;

import io.atomix.utils.config.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Messaging configuration. */
public class MessagingConfig implements Config {
  private static final int DEFAULT_MAX_MESSAGE_SIZE_BYTES = 4 * 1024 * 1024;

  private final int connectionPoolSize = 8;
  private List<String> interfaces = new ArrayList<>();
  private Integer port;
  private Duration shutdownQuietPeriod = Duration.ofMillis(20);
  private Duration shutdownTimeout = Duration.ofSeconds(1);
  private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE_BYTES;

  /**
   * Returns the local interfaces to which to bind the node.
   *
   * @return the local interfaces to which to bind the node
   */
  public List<String> getInterfaces() {
    return interfaces;
  }

  /**
   * Sets the local interfaces to which to bind the node.
   *
   * @param interfaces the local interfaces to which to bind the node
   * @return the local cluster configuration
   */
  public MessagingConfig setInterfaces(final List<String> interfaces) {
    this.interfaces = interfaces;
    return this;
  }

  /**
   * Returns the local port to which to bind the node.
   *
   * @return the local port to which to bind the node
   */
  public Integer getPort() {
    return port;
  }

  /**
   * Sets the local port to which to bind the node.
   *
   * @param port the local port to which to bind the node
   * @return the local cluster configuration
   */
  public MessagingConfig setPort(final Integer port) {
    this.port = port;
    return this;
  }

  /**
   * Returns the connection pool size.
   *
   * @return the connection pool size
   */
  public int getConnectionPoolSize() {
    return connectionPoolSize;
  }

  /** @return the configured shutdown quiet period */
  public Duration getShutdownQuietPeriod() {
    return shutdownQuietPeriod;
  }

  /**
   * Sets the shutdown quiet period. This is mostly useful to set a small value when testing,
   * otherwise every tests takes an additional 2 second just to shutdown the executor.
   *
   * @param shutdownQuietPeriod the quiet period on shutdown
   * @return this config
   */
  public MessagingConfig setShutdownQuietPeriod(final Duration shutdownQuietPeriod) {
    this.shutdownQuietPeriod = shutdownQuietPeriod;
    return this;
  }

  /** @return the configured shutdown timeout */
  public Duration getShutdownTimeout() {
    return shutdownTimeout;
  }

  /**
   * Sets the shutdown timeout.
   *
   * @param shutdownTimeout the time to wait for an orderly shutdown of the messaging service
   * @return this config
   */
  public MessagingConfig setShutdownTimeout(final Duration shutdownTimeout) {
    this.shutdownTimeout = shutdownTimeout;
    return this;
  }

  /** @return the maximum size of a network message */
  public int getMaxMessageSize() {
    return maxMessageSize;
  }

  /**
   * Convenience method accepting a long value instead of an int. If the given value is greater than
   * {@link Integer#MAX_VALUE}, it will fallback to that instead.
   *
   * @param maxMessageSizeBytes the new maximum size of a serialized network message
   * @see #setMaxMessageSize(int)
   * @return this config for chaining
   */
  public MessagingConfig setMaxMessageSize(final long maxMessageSizeBytes) {
    return setMaxMessageSize((int) Math.min(Integer.MAX_VALUE, maxMessageSizeBytes));
  }

  /**
   * Sets the maximum size of a network message. Messages which exceed this size on either the
   * client or the server will trigger a RESOURCE_EXHAUSTED error.
   *
   * @param maxMessageSizeBytes the new maximum size of a serialized network message
   * @return this config for chaining
   */
  public MessagingConfig setMaxMessageSize(final int maxMessageSizeBytes) {
    this.maxMessageSize = maxMessageSizeBytes;
    return this;
  }
}
