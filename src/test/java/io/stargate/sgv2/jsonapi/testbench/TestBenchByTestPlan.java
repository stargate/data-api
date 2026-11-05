package io.stargate.sgv2.jsonapi.testbench;

import io.stargate.sgv2.jsonapi.testbench.testspec.SpecFiles;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for running a Test Bench from a Test Plan file.
 *
 * <p>Put the name of the test plan file in the <code>TEST_PLAN_FILE</code> env var, this can set
 * the target to hit, and the workflows to run. See {@link TestPlanFile}
 * </p>
 * <p>
 *  This will look like a unit test, so we only run when the env var is set.
 * </p>
 */
public class TestBenchByTestPlan {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBenchByTestPlan.class);

  @TestFactory
  public Stream<DynamicNode> runTestPlanFile() {

    var rawPath = System.getenv("TEST_PLAN_FILE");
    if (rawPath == null) {
      return Stream.empty();
    }
    LOGGER.info("runTestPlanFile() - getting TEST_PLAN_FILE from ENV, rawPath={}", rawPath);

    var path =
        rawPath.startsWith("classpath:")
            ? SpecFiles.resourceDir(rawPath.substring("classpath:".length()))
            : Path.of(rawPath);

    var testPlan = TestBenchPlan.fromFile(path);
    LOGGER.info("runTestPlanFile() - building test plan tree");
    var testPlanNodeTree = testPlan.testNode();

    LOGGER.info(
        "runTestPlanFile() - test plan tree build, totalNodeCount={}",
        testPlanNodeTree.totalNodeCount());
    System.setProperty(
        TestBenchPlan.TEST_PLAN_TEST_COUNT_PROPERTY,
        String.valueOf(testPlanNodeTree.totalNodeCount()));
    return Stream.of(testPlanNodeTree.root());
  }
}
