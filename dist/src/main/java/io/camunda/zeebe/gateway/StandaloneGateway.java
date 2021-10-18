/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway;

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.protocol.GroupMembershipProtocol;
import io.atomix.cluster.protocol.SwimMembershipProtocol;
import io.atomix.utils.net.Address;
import io.camunda.zeebe.gateway.impl.SpringGatewayBridge;
import io.camunda.zeebe.gateway.impl.broker.BrokerClient;
import io.camunda.zeebe.gateway.impl.broker.BrokerClientImpl;
import io.camunda.zeebe.gateway.impl.broker.cluster.BrokerTopologyManager;
import io.camunda.zeebe.gateway.impl.configuration.ClusterCfg;
import io.camunda.zeebe.gateway.impl.configuration.GatewayCfg;
import io.camunda.zeebe.gateway.impl.configuration.MembershipCfg;
import io.camunda.zeebe.shared.Profile;
import io.camunda.zeebe.util.VersionUtil;
import io.camunda.zeebe.util.sched.ActorScheduler;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

/**
 * Entry point for the standalone gateway application. By default, it enables the {@link
 * Profile#GATEWAY} profile, loading the appropriate application properties overrides.
 *
 * <p>See {@link #main(String[])} for more.
 */
@SpringBootApplication(
    scanBasePackages = {
      "io.camunda.zeebe.gateway",
      "io.camunda.zeebe.shared",
      "io.camunda.zeebe.util.liveness"
    })
public class StandaloneGateway
    implements CommandLineRunner, ApplicationListener<ContextClosedEvent> {
  private static final Logger LOG = Loggers.GATEWAY_LOGGER;

  private final GatewayCfg configuration;
  private final SpringGatewayBridge springGatewayBridge;

  private AtomixCluster atomixCluster;
  private Gateway gateway;
  private ActorScheduler actorScheduler;

  @Autowired
  public StandaloneGateway(
      final GatewayCfg configuration, final SpringGatewayBridge springGatewayBridge) {
    this.configuration = configuration;
    this.springGatewayBridge = springGatewayBridge;
  }

  public static void main(final String[] args) {
    System.setProperty("spring.banner.location", "classpath:/assets/zeebe_gateway_banner.txt");
    final var application =
        new SpringApplicationBuilder(StandaloneGateway.class)
            .web(WebApplicationType.SERVLET)
            .logStartupInfo(true)
            .profiles(Profile.GATEWAY.getId())
            .build(args);

    application.run();
  }

  @Override
  public void run(final String... args) throws Exception {
    configuration.init();

    if (LOG.isInfoEnabled()) {
      LOG.info("Version: {}", VersionUtil.getVersion());
      LOG.info("Starting standalone gateway with configuration {}", configuration.toJson());
    }

    atomixCluster = createAtomixCluster(configuration.getCluster());
    actorScheduler = createActorScheduler(configuration);
    gateway = new Gateway(configuration, this::createBrokerClient, actorScheduler);

    springGatewayBridge.registerGatewayStatusSupplier(gateway::getStatus);
    springGatewayBridge.registerClusterStateSupplier(
        () ->
            Optional.ofNullable(gateway.getBrokerClient())
                .map(BrokerClient::getTopologyManager)
                .map(BrokerTopologyManager::getTopology));

    actorScheduler.start();
    atomixCluster.start();
    gateway.start();
  }

  @Override
  public void onApplicationEvent(final ContextClosedEvent event) {
    if (gateway != null) {
      try {
        gateway.stop();
      } catch (final Exception e) {
        LOG.warn("Failed to gracefully shutdown gRPC gateway", e);
      }
    }

    if (atomixCluster != null) {
      try {
        atomixCluster.stop().orTimeout(10, TimeUnit.SECONDS).join();
      } catch (final Exception e) {
        LOG.warn("Failed to gracefully shutdown cluster services", e);
      }
    }

    if (actorScheduler != null) {
      try {
        actorScheduler.close();
      } catch (final Exception e) {
        LOG.warn("Failed to gracefully shutdown actor scheduler", e);
      }
    }

    LogManager.shutdown();
  }

  private BrokerClient createBrokerClient(final GatewayCfg config) {
    return new BrokerClientImpl(
        config,
        atomixCluster.getMessagingService(),
        atomixCluster.getMembershipService(),
        atomixCluster.getEventService(),
        actorScheduler,
        false);
  }

  private AtomixCluster createAtomixCluster(final ClusterCfg config) {
    final var membershipProtocol = createMembershipProtocol(config.getMembership());

    return AtomixCluster.builder()
        .withMemberId(config.getMemberId())
        .withAddress(Address.from(config.getHost(), config.getPort()))
        .withClusterId(config.getClusterName())
        .withMembershipProvider(
            BootstrapDiscoveryProvider.builder()
                .withNodes(Address.from(config.getContactPoint()))
                .build())
        .withMembershipProtocol(membershipProtocol)
        .build();
  }

  private GroupMembershipProtocol createMembershipProtocol(final MembershipCfg config) {
    return SwimMembershipProtocol.builder()
        .withFailureTimeout(config.getFailureTimeout())
        .withGossipInterval(config.getGossipInterval())
        .withProbeInterval(config.getProbeInterval())
        .withProbeTimeout(config.getProbeTimeout())
        .withBroadcastDisputes(config.isBroadcastDisputes())
        .withBroadcastUpdates(config.isBroadcastUpdates())
        .withGossipFanout(config.getGossipFanout())
        .withNotifySuspect(config.isNotifySuspect())
        .withSuspectProbes(config.getSuspectProbes())
        .withSyncInterval(config.getSyncInterval())
        .build();
  }

  private ActorScheduler createActorScheduler(final GatewayCfg config) {
    return ActorScheduler.newActorScheduler()
        .setCpuBoundActorThreadCount(config.getThreads().getManagementThreads())
        .setIoBoundActorThreadCount(0)
        .setSchedulerName("gateway-scheduler")
        .build();
  }
}
