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
import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.zeebe.containers.ZeebeGatewayContainer;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class GatewayGrpcHealthIntegrationTest {

  private final Network network = Network.newNetwork();

  @Container
  private final ZeebeGatewayContainer zeebeGatewayContainer =
      new ZeebeGatewayContainer(ZeebeTestContainerDefaults.defaultTestImage())
          .withoutTopologyCheck()
          .withNetwork(network);

  @ParameterizedTest
  @ValueSource(strings = {GatewayGrpc.SERVICE_NAME, HealthStatusManager.SERVICE_NAME_ALL_SERVICES})
  void shouldReturnServingWhenGatewayIsStarted(final String serviceName) {
    ManagedChannel channel = null;

    try {
      channel =
          NettyChannelBuilder.forTarget(zeebeGatewayContainer.getExternalGatewayAddress())
              .usePlaintext()
              .build();
      final var client = HealthGrpc.newBlockingStub(channel);
      final var response =
          client.check(HealthCheckRequest.newBuilder().setService(serviceName).build());
      assertThat(response.getStatus()).isEqualTo(ServingStatus.SERVING);
    } finally {
      Optional.ofNullable(channel).ifPresent(ManagedChannel::shutdownNow);
    }
  }
}
