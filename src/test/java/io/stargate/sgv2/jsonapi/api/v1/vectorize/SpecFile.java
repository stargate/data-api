package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.JsonNode;

public class SpecFile {

  private final TestSpec spec;
  private final JsonNode rootNode;

  SpecFile(TestSpec spec, JsonNode rootNode) {
    this.spec = spec;
    this.rootNode = rootNode;
  }

  public TestSpec spec() {
    return spec;
  }

  public JsonNode root() {
    return rootNode;
  }
}
