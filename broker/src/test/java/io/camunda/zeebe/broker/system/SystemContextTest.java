/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.system;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.camunda.zeebe.broker.system.configuration.partitioning.FixedPartitionCfg;
import io.camunda.zeebe.broker.system.configuration.partitioning.FixedPartitionCfg.NodeCfg;
import io.camunda.zeebe.broker.system.configuration.partitioning.Scheme;
import io.camunda.zeebe.util.sched.clock.ControlledActorClock;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;

final class SystemContextTest {

  @Test
  void shouldThrowExceptionIfNodeIdIsNegative() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setNodeId(-1);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Node id -1 needs to be non negative and smaller then cluster size 1.");
  }

  @Test
  void shouldThrowExceptionIfNodeIdIsLargerThenClusterSize() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setNodeId(2);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Node id 2 needs to be non negative and smaller then cluster size 1.");
  }

  @Test
  void shouldThrowExceptionIfReplicationFactorIsNegative() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setReplicationFactor(-1);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Replication factor -1 needs to be larger then zero and not larger then cluster size 1.");
  }

  @Test
  void shouldThrowExceptionIfReplicationFactorIsLargerThenClusterSize() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setReplicationFactor(2);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Replication factor 2 needs to be larger then zero and not larger then cluster size 1.");
  }

  @Test
  void shouldThrowExceptionIfPartitionsCountIsNegative() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setPartitionsCount(-1);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Partition count must not be smaller then 1.");
  }

  @Test
  void shouldThrowExceptionIfSnapshotPeriodIsNegative() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getData().setSnapshotPeriod(Duration.ofMinutes(-1));

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshot period PT-1M needs to be larger then or equals to one minute.");
  }

  @Test
  void shouldThrowExceptionIfSnapshotPeriodIsTooSmall() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getData().setSnapshotPeriod(Duration.ofSeconds(1));

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshot period PT1S needs to be larger then or equals to one minute.");
  }

  @Test
  void shouldThrowExceptionIfBatchSizeIsNegative() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getExperimental().setMaxAppendBatchSize(DataSize.of(-1, DataUnit.BYTES));

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Expected to have an append batch size maximum which is non negative and smaller then '2147483647', but was '-1B'.");
  }

  @Test
  void shouldThrowExceptionIfBatchSizeIsTooLarge() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getExperimental().setMaxAppendBatchSize(DataSize.of(3, DataUnit.GIGABYTES));

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Expected to have an append batch size maximum which is non negative and smaller then '2147483647', but was '3221225472B'.");
  }

  @Test
  void shouldNotThrowExceptionIfSnapshotPeriodIsEqualToOneMinute() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getData().setSnapshotPeriod(Duration.ofMinutes(1));

    // when
    final var systemContext = initSystemContext(brokerCfg);

    // then
    assertThat(systemContext.getBrokerConfiguration().getData().getSnapshotPeriod())
        .isEqualTo(Duration.ofMinutes(1));
  }

  @Test
  void shouldThrowExceptionIfHeartbeatIntervalIsSmallerThanOneMs() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setHeartbeatInterval(Duration.ofMillis(0));

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("heartbeatInterval PT0S must be at least 1ms");
  }

  @Test
  void shouldThrowExceptionIfElectionTimeoutIsSmallerThanOneMs() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setElectionTimeout(Duration.ofMillis(0));

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("electionTimeout PT0S must be at least 1ms");
  }

  @Test
  void shouldThrowExceptionIfElectionTimeoutIsSmallerThanHeartbeatInterval() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    brokerCfg.getCluster().setElectionTimeout(Duration.ofSeconds(1));
    brokerCfg.getCluster().setHeartbeatInterval(Duration.ofSeconds(2));

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("electionTimeout PT1S must be greater than heartbeatInterval PT2S");
  }

  @Test
  void shouldThrowExceptionIfFixedPartitioningSchemeDoesNotSpecifyAnyPartitions() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    final var config = brokerCfg.getExperimental().getPartitioning();
    config.setScheme(Scheme.FIXED);
    config.setFixed(List.of());

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Expected fixed partition scheme to define configurations for all partitions such that "
                + "they have 1 replicas, but partition 1 has 0 configured replicas: []");
  }

  @Test
  void shouldThrowExceptionIfFixedPartitioningSchemeIsNotExhaustive() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    final var config = brokerCfg.getExperimental().getPartitioning();
    final var fixedPartition = new FixedPartitionCfg();
    config.setScheme(Scheme.FIXED);
    config.setFixed(List.of(fixedPartition));
    fixedPartition.getNodes().add(new NodeCfg());
    brokerCfg.getCluster().setPartitionsCount(2);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Expected fixed partition scheme to define configurations for all partitions such that "
                + "they have 1 replicas, but partition 2 has 0 configured replicas: []");
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 2})
  void shouldThrowExceptionIfFixedPartitioningSchemeUsesInvalidPartitionId(final int invalidId) {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    final var config = brokerCfg.getExperimental().getPartitioning();
    final var fixedPartition = new FixedPartitionCfg();
    config.setScheme(Scheme.FIXED);
    config.setFixed(List.of(fixedPartition));
    fixedPartition.setPartitionId(invalidId);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Expected fixed partition scheme to define entries with a valid partitionId "
                + "between 1 and 1, but %d was given",
            invalidId);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 2})
  void shouldThrowExceptionIfFixedPartitioningSchemeUsesInvalidNodeId(final int invalidId) {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    final var config = brokerCfg.getExperimental().getPartitioning();
    final var fixedPartition = new FixedPartitionCfg();
    final var node = new NodeCfg();
    config.setScheme(Scheme.FIXED);
    config.setFixed(List.of(fixedPartition));
    fixedPartition.getNodes().add(node);
    node.setNodeId(invalidId);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Expected fixed partition scheme for partition 1 to define nodes with a "
                + "nodeId between 0 and 0, but it was %d",
            invalidId);
  }

  @Test
  void shouldThrowExceptionIfFixedPartitioningSchemeHasSamePriorities() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    final var config = brokerCfg.getExperimental().getPartitioning();
    final var fixedPartition = new FixedPartitionCfg();
    final var nodes = List.of(new NodeCfg(), new NodeCfg());
    brokerCfg.getExperimental().setEnablePriorityElection(true);
    brokerCfg.getCluster().setClusterSize(2);
    config.setScheme(Scheme.FIXED);
    config.setFixed(List.of(fixedPartition));
    fixedPartition.setNodes(nodes);
    nodes.get(0).setNodeId(0);
    nodes.get(0).setPriority(1);
    nodes.get(1).setNodeId(1);
    nodes.get(1).setPriority(1);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Expected each node for a partition 1 to have a different priority, but at least two of"
                + " them have the same priorities: [NodeCfg{nodeId=0, priority=1},"
                + " NodeCfg{nodeId=1, priority=1}]");
  }

  @Test
  void shouldNotThrowExceptionIfFixedPartitioningSchemeHasWrongPrioritiesWhenPriorityDisabled() {
    // given
    final BrokerCfg brokerCfg = new BrokerCfg();
    final var config = brokerCfg.getExperimental().getPartitioning();
    final var fixedPartition = new FixedPartitionCfg();
    final var nodes = List.of(new NodeCfg(), new NodeCfg());
    brokerCfg.getExperimental().setEnablePriorityElection(false);
    brokerCfg.getCluster().setClusterSize(2);
    config.setScheme(Scheme.FIXED);
    config.setFixed(List.of(fixedPartition));
    fixedPartition.setNodes(nodes);
    nodes.get(0).setNodeId(0);
    nodes.get(0).setPriority(1);
    nodes.get(1).setNodeId(1);
    nodes.get(1).setPriority(1);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg)).doesNotThrowAnyException();
  }

  @Test
  void shouldThrowExceptionWithNetworkSecurityEnabledAndWrongCert() throws CertificateException {
    // given
    final var certificate = new SelfSignedCertificate();
    final var brokerCfg = new BrokerCfg();
    brokerCfg
        .getNetwork()
        .getSecurity()
        .setEnabled(true)
        .setPrivateKeyPath(certificate.privateKey().getAbsolutePath())
        .setCertificateChainPath("/tmp/i-dont-exist.crt");

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Expected the configured network security certificate chain path "
                + "'/tmp/i-dont-exist.crt' to point to a readable file, but it does not");
  }

  @Test
  void shouldThrowExceptionWithNetworkSecurityEnabledAndWrongKey() throws CertificateException {
    // given
    final var certificate = new SelfSignedCertificate();
    final var brokerCfg = new BrokerCfg();
    brokerCfg
        .getNetwork()
        .getSecurity()
        .setEnabled(true)
        .setPrivateKeyPath("/tmp/i-dont-exist.key")
        .setCertificateChainPath(certificate.certificate().getAbsolutePath());

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Expected the configured network security private key path "
                + "'/tmp/i-dont-exist.key' to point to a readable file, but it does not");
  }

  @Test
  void shouldThrowExceptionWithNetworkSecurityEnabledAndNoPrivateKey() throws CertificateException {
    // given
    final var certificate = new SelfSignedCertificate();
    final var brokerCfg = new BrokerCfg();
    brokerCfg
        .getNetwork()
        .getSecurity()
        .setEnabled(true)
        .setPrivateKeyPath(null)
        .setCertificateChainPath(certificate.certificate().getAbsolutePath());

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Expected to have a valid private key path for network security, but none configured");
  }

  @Test
  void shouldThrowExceptionWithNetworkSecurityEnabledAndNoCert() throws CertificateException {
    // given
    final var certificate = new SelfSignedCertificate();
    final var brokerCfg = new BrokerCfg();
    brokerCfg
        .getNetwork()
        .getSecurity()
        .setEnabled(true)
        .setPrivateKeyPath(certificate.privateKey().getAbsolutePath())
        .setCertificateChainPath(null);

    // when - then
    assertThatCode(() -> initSystemContext(brokerCfg))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Expected to have a valid certificate chain path for network security, but none "
                + "configured");
  }

  private SystemContext initSystemContext(final BrokerCfg brokerCfg) {
    return new SystemContext(brokerCfg, "test", new ControlledActorClock());
  }
}
