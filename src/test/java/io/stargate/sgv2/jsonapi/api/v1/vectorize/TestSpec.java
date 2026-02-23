package io.stargate.sgv2.jsonapi.api.v1.vectorize;

public interface TestSpec {

  TestSpecKind kind();

  TestSpecMeta meta();

  //  void setJson(JsonNode json);

  enum TestSpecKind {
    TEST,
    WORKFLOW
  }
}
