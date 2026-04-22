package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TargetsSpec;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBenchTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBenchTests.class);

  @TestFactory
  public Stream<DynamicNode> runTestPlanFile() {

    var rawPath = System.getenv("TEST_PLAN_FILE");
    LOGGER.info("runTestPlanFile() - getting TEST_PLAN_FILE from ENV, rawPath={}", rawPath);

    var path =
        rawPath.startsWith("classpath:")
            ? TargetsSpec.resourceDir(rawPath.substring("classpath:".length()))
            : Path.of(rawPath);

    var testPlan = TestPlan.fromFile(path);
    LOGGER.info("runTestPlanFile() - building test plan tree");
    var testPlanNodeTree = testPlan.testNode();

    LOGGER.info("runTestPlanFile() - test plan tree build, totalNodeCount={}", testPlanNodeTree.totalNodeCount());
    System.setProperty("testbench.test.count", String.valueOf(testPlanNodeTree.totalNodeCount()));
    return Stream.of(testPlanNodeTree.root());
  }

  private static int countAllNodes(int accum, Stream<DynamicNode> nodes) {
    return nodes.reduce(accum, (acc, node) -> switch (node) {
      case DynamicTest t -> acc + 1;
      case DynamicContainer c -> countAllNodes(acc, c.getChildren().map(n -> (DynamicNode) n));
      default -> acc;
    }, Integer::sum);
  }


  //
  //  public static void main(String[] args) {
  //    LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
  //            .selectors(DiscoverySelectors.selectClass(TestBenchTests.class))
  //            .build();
  //
  //    Launcher launcher = LauncherFactory.create();
  //    launcher.execute(request, new DynamicTreeListener());
  //  }
}
