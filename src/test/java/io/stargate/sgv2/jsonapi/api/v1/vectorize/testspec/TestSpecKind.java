package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

public enum TestSpecKind {
  ASSERTION_TEMPLATE,
  TEST_SUITE,
  WORKFLOW;

  public static <T extends TestSpec> TestSpecKind fromType(Class<T> clazz) {
    if (clazz == AssertionTemplateSpec.class) { return ASSERTION_TEMPLATE; }
    if (clazz == TestSuite.class)         { return TEST_SUITE; }
    if (clazz == Workflow.class)          { return WORKFLOW; }
    throw new IllegalArgumentException("Unknown TestSpec type: " + clazz);
  }
}
