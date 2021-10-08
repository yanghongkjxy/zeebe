/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.it.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.gateway.protocol.GatewayGrpc;
import io.camunda.zeebe.test.util.testcontainers.ZeebeTestContainerDefaults;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.zeebe.containers.ZeebeGatewayContainer;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class GatewayGrpcHealthIT {

  @Container
  private final ZeebeGatewayContainer zeebeGatewayContainer =
      new ZeebeGatewayContainer(ZeebeTestContainerDefaults.defaultTestImage())
          .withoutTopologyCheck();

  @ParameterizedTest
  @ValueSource(strings = {GatewayGrpc.SERVICE_NAME, HealthStatusManager.SERVICE_NAME_ALL_SERVICES})
  void shouldReturnServingStatusWhenGatewayIsStarted(final String serviceName) {
    try (final var channel =
        new ManagedChannelAutoClosableWrapper(
            NettyChannelBuilder.forTarget(zeebeGatewayContainer.getExternalGatewayAddress())
                .usePlaintext()
                .build())) {
      final var client = HealthGrpc.newBlockingStub(channel);
      final var response =
          client.check(HealthCheckRequest.newBuilder().setService(serviceName).build());
      assertThat(response.getStatus()).isEqualTo(ServingStatus.SERVING);
    }
  }

  private static final class ManagedChannelAutoClosableWrapper extends ManagedChannel
      implements AutoCloseable {

    private final ManagedChannel innerChannel;

    private ManagedChannelAutoClosableWrapper(final ManagedChannel innerChannel) {
      this.innerChannel = innerChannel;
    }

    @Override
    public ManagedChannel shutdown() {
      return this.innerChannel.shutdown();
    }

    @Override
    public boolean isShutdown() {
      return this.innerChannel.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return this.innerChannel.isTerminated();
    }

    @Override
    public ManagedChannel shutdownNow() {
      return this.innerChannel.shutdownNow();
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit timeUnit)
        throws InterruptedException {
      return this.innerChannel.awaitTermination(timeout, timeUnit);
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        final MethodDescriptor<RequestT, ResponseT> methodDescriptor,
        final CallOptions callOptions) {
      return this.innerChannel.newCall(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
      return this.innerChannel.authority();
    }

    @Override
    public void close() {
      this.innerChannel.shutdownNow();
    }
  }
}
