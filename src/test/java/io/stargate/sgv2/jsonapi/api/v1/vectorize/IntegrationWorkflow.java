package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.List;

public record IntegrationWorkflow(ITMetadata meta, List<IntegrationJob> jobs) implements ITElement {

  @Override
  public ITElementKind kind() {
    return ITElementKind.WORKFLOW;
  }
}
