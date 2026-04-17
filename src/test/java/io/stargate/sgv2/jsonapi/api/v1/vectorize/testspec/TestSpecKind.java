package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

public enum TestSpecKind {
  ASSERTION_TEMPLATE,
  TARGETS,
  TEST_SUITE,
  WORKFLOW;

  public static <T extends TestSpec> TestSpecKind fromType(Class<T> clazz) {
    if (clazz == AssertionTemplateSpec.class) {
      return ASSERTION_TEMPLATE;
    }
    if (clazz == TargetsSpec.class) {
      return TARGETS;
    }
    if (clazz == TestSuiteSpec.class) {
      return TEST_SUITE;
    }
    if (clazz == WorkflowSpec.class) {
      return WORKFLOW;
    }
    throw new IllegalArgumentException("Unknown TestSpec type: " + clazz);
  }
}
