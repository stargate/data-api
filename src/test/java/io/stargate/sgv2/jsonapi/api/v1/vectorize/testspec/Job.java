package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record Job(
    TestSpecMeta meta,
    Map<String, String> fromEnvironment,
    Map<String, String> variables,
    Map<String, List<String>> matrix,
    List<String> tests) {

  private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

  public DynamicContainer testNode(TestPlan testPlan, TestUri.Builder uriBuilder) {

    uriBuilder.addSegment(TestUri.Segment.JOB, meta.name());

    var desc = "Job: %s ".formatted(meta.name());

    testPlan.updateJobForTarget(this);
    var allEnvs = allEnvironments(testPlan);
    var testSuiteNodes =
        testSuites(testPlan)
            .map(testSuite -> testSuite.testNode(testPlan, uriBuilder.clone(), allEnvs));

    return dynamicContainer(
        desc,
        uriBuilder.build().uri(),
        testPlan.addLifecycle(uriBuilder.clone(), this, testSuiteNodes));
  }

  public Stream<TestSuiteSpec> testSuites(TestPlan testPlan) {
    Stream.Builder<TestSuiteSpec> allTests = Stream.builder();
    tests()
        .forEach(
            testName -> {
              testPlan.specFiles().byNameAsType(TestSuiteSpec.class, testName).forEach(allTests);
            });

    return allTests.build();
  }

  public TestRunEnv withoutMatrix(TestPlan testPlan) {

    var fromEnv = new TestRunEnv();

    for (Map.Entry<String, String> entry : fromEnvironment.entrySet()) {
      fromEnv.put(entry.getKey(), TestEnvAccess.getEnvVar(entry.getKey()));
    }

    var fromVariables = new TestRunEnv(variables);

    return fromEnv.clone().put(fromVariables);
  }

  public List<TestRunEnv> allEnvironments(TestPlan testPlan) {

    var fromEnv = new TestRunEnv();

    for (Map.Entry<String, String> entry : fromEnvironment.entrySet()) {
      fromEnv.put(entry.getKey(), TestEnvAccess.getEnvVar(entry.getValue()));
    }

    var fromVariables = new TestRunEnv(variables);

    // TODO: handle more matrix
    List<TestRunEnv> fromMatrix = new ArrayList<>();
    matrix
        .get("MODEL")
        .forEach(
            model -> {
              var env = new TestRunEnv();
              env.put("MODEL", model);
              fromMatrix.add(env);
            });

    List<TestRunEnv> allEnvs = new ArrayList<>();
    for (var matrixEnv : fromMatrix) {

      var completeEnv = fromEnv.clone().put(fromVariables).put(matrixEnv);

      allEnvs.add(completeEnv);
    }

    return allEnvs;
  }
}
