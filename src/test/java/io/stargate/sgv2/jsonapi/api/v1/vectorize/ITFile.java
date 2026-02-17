package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.JsonNode;

public class ITFile {

  private final ITElement element;
  private final JsonNode root;

  ITFile(ITElement element, JsonNode root) {
    this.element = element;
    this.root = root;
  }

  public ITElement element() {
    return element;
  }

  public JsonNode root() {
    return root;
  }
}
