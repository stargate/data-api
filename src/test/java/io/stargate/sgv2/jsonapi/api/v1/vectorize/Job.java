package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSpecKind;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSpecMeta;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TestSuite;
import org.junit.jupiter.api.DynamicContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

public record Job(

    TestSpecMeta meta,
    Map<String, String> fromEnvironment,
    Map<String, String> variables,
    Map<String, List<String>> matrix,
    List<String> tests) {

  private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);


  public DynamicContainer testNode(TestPlan testPlan) {

    var desc = "Job: %s ".formatted(
        meta.name());

    testPlan.updateJobForTarget(this);
    var allEnvs = allEnvironments(testPlan);
    var testSuiteNodes =  testSuites(testPlan)
        .map(testSuite -> testSuite.testNode(testPlan, allEnvs));

    return  dynamicContainer(
            desc,
            testPlan.addLifecycle(this, testSuiteNodes));
  }

  public Stream<TestSuite> testSuites(TestPlan testPlan) {
    Stream.Builder<TestSuite> allTests =  Stream.builder();
    tests()
        .forEach(
            testName -> {
               testPlan.specFiles().byNameAsType(TestSuite.class, testName).forEach(allTests) ;
            });

    return allTests.build();
  }
  public TestEnvironment withoutMatrix(TestPlan testPlan) {

    var fromEnv = new TestEnvironment();

    for (Map.Entry<String, String> entry : fromEnvironment.entrySet()) {
      var value = System.getenv(entry.getValue());
      if (value== null) {
        throw new RuntimeException("Environment variable " + entry.getValue() + " is undefined");
      }
      fromEnv.put(entry.getKey(), value);
    }

    var fromVariables = new TestEnvironment(variables);

    return fromEnv.clone().put(fromVariables);
  }
  public List<TestEnvironment> allEnvironments(TestPlan testPlan) {

    var fromEnv = new TestEnvironment();

    for (Map.Entry<String, String> entry : fromEnvironment.entrySet()) {
      var value = System.getenv(entry.getValue());
      if (value== null) {
        throw new RuntimeException("Environment variable " + entry.getValue() + " is undefined");
      }
      fromEnv.put(entry.getKey(), value);
    }

    var fromVariables = new TestEnvironment(variables);

    // TODO: handle more matrix
    List<TestEnvironment> fromMatrix = new ArrayList<>();
    matrix.get("MODEL").forEach(
        model -> {
          var env = new TestEnvironment();
          env.put("MODEL", model);
          fromMatrix.add(env);
        });

    List<TestEnvironment> allEnvs = new ArrayList<>();
    for (var matrixEnv : fromMatrix) {

      var completeEnv = fromEnv
          .clone()
          .put(fromVariables)
          .put(matrixEnv);

      allEnvs.add(completeEnv);
    }

    return allEnvs;
  }
}
