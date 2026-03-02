package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

public sealed interface TestSpec permits  Workflow, TestSuite, AssertionTemplateSpec {

  TestSpecMeta meta();

  default <T extends TestSpec> T asSpecType(Class<T> type) {

    if (!type.isInstance(this)) {
      throw new IllegalArgumentException(
          "TestSpec is not of required type. expected=%s, spec.meta=%s".formatted(type, meta()));
    }
    return type.cast(this);
  }
}
