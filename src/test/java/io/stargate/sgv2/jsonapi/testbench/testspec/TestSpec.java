package io.stargate.sgv2.jsonapi.testbench.testspec;

/**
 * A specification for objects in the TestBench world, such as a test suite.
 * <p>
 * Implement this for any object types a {@link io.stargate.sgv2.jsonapi.testbench.TestPlan} needs
 * to read from disk or know about.
 * </p>
 */
public sealed interface TestSpec
    permits AssertionTemplateSpec, TargetsSpec, TestSuiteSpec, WorkflowSpec {

  TestSpecMeta meta();

  /**
   * Gets the object as the  implementing class
   * @param type the implementing class of the {@link TestSpec}
   * @return The implementing object, as the type.
   * @param <T> the implementing class of the {@link TestSpec}
   */
  default <T extends TestSpec> T asSpecType(Class<T> type) {

    if (!type.isInstance(this)) {
      throw new IllegalArgumentException(
          "TestSpec is not of required type. expected=%s, spec.meta=%s".formatted(type, meta()));
    }
    return type.cast(this);
  }
}
