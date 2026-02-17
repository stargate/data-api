package io.stargate.sgv2.jsonapi.api.v1.vectorize;

public interface ITElement {

  ITElementKind kind();

  ITMetadata meta();

  //  void setJson(JsonNode json);

  enum ITElementKind {
    TEST,
    WORKFLOW
  }
}
