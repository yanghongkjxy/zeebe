/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.exporter.stream;

import io.prometheus.client.Counter;

public class ExporterMetrics {

  private static final Counter EXPORTER_EVENTS =
      Counter.build()
          .namespace("zeebe")
          .name("exporter_events_total")
          .help("Number of events processed by exporter")
          .labelNames("action", "partition", "valueType")
          .register();

  private final String partitionIdLabel;

  public ExporterMetrics(int partitionId) {
    partitionIdLabel = String.valueOf(partitionId);
  }

  private void event(String action, String valueType) {
    EXPORTER_EVENTS.labels(action, partitionIdLabel, valueType).inc();
  }

  public void eventExported(String valueType) {
    event("exported", valueType);
  }

  public void eventSkipped(String valueType) {
    event("skipped", valueType);
  }
}
