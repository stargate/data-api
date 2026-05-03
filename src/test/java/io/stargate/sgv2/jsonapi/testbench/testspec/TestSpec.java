package io.stargate.sgv2.jsonapi.testbench.testspec;

public sealed interface TestSpec
    permits AssertionTemplateSpec, TargetsSpec, TestSuiteSpec, WorkflowSpec {

  TestSpecMeta meta();

  default <T extends TestSpec> T asSpecType(Class<T> type) {

    if (!type.isInstance(this)) {
      throw new IllegalArgumentException(
          "TestSpec is not of required type. expected=%s, spec.meta=%s".formatted(type, meta()));
    }
    return type.cast(this);
  }
}
