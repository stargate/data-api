package io.stargate.sgv2.jsonapi.testbench.testrun;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.function.Executable;

public class DynamicTestExecutable implements Executable {

  private final String description;
  private final TestUri testUri;
  private final Executable executable;

  private final String trimmedDisplayName;
  private final boolean isTrimmed;
  private final TestExecutionCondition testExecutionCondition;

  public DynamicTestExecutable(String description, TestUri.Builder testUri, TestExecutionCondition testExecutionCondition, Executable executable) {
    this(description, testUri.build(), testExecutionCondition, executable);
  }

  @SuppressWarnings("StringEquality")
  public DynamicTestExecutable(String description, TestUri testUri, TestExecutionCondition testExecutionCondition, Executable executable) {
    this.description = description;
    this.testUri = testUri;
    this.testExecutionCondition = testExecutionCondition;
    this.executable = executable;

    var truncated =
        (description != null && description.length() > 120)
            ? description.substring(0, 117) + "..."
            : description;

    this.trimmedDisplayName = truncated;
    // using reference quality to see it is a diff object.
    this.isTrimmed = truncated != description;
  }

  public String trimmedDisplayName() {
    return trimmedDisplayName;
  }

  public DynamicTest testNode(TestNodeFactory testNodeFactory) {
    return testNodeFactory.testPlanTest(trimmedDisplayName, testUri.uri(), this);
  }

  @Override
  public void execute() throws Throwable {
    Assumptions.assumeTrue(testExecutionCondition,  testExecutionCondition.message());
    beforeExecute();
    try {
      executable.execute();
    }
    catch (Throwable e) {
      testExecutionCondition.abortFutureTests("Failed Upstream: " + trimmedDisplayName);
      throw e;
    }
    afterExecute();
  }

  private void beforeExecute() {
    //    if (isTrimmed) {
    //      System.out.printf(description + "\n");
    //    }
  }

  private void afterExecute() {
    //    System.out.printf("Executed - " + testUri.uri().toString() + "\n");
  }
}
