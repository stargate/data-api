package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.TargetsSpec;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBenchTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBenchTests.class);

  @TestFactory
  public Stream<DynamicContainer> runTestPlanFile() {

    var rawPath = System.getenv("TEST_PLAN_FILE");
    LOGGER.info("runTestPlanFile() - getting TEST_PLAN_FILE from ENV, rawPath={}", rawPath);

    var path =
        rawPath.startsWith("classpath:")
            ? TargetsSpec.resourceDir(rawPath.substring("classpath:".length()))
            : Path.of(rawPath);

    var testContext = TestPlan.fromFile(path);

    return testContext.testPlan().testNode();
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
