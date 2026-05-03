package io.stargate.sgv2.jsonapi.testbench;



import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.stargate.sgv2.jsonapi.testbench.targets.Target;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import io.stargate.sgv2.jsonapi.testbench.testspec.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.DynamicNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record TestPlan(
        Target target, SpecFiles specFiles, Set<String> workflows, boolean ignoreDisabled) {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestPlan.class);

  private static final ObjectMapper JSON_MAPPER =
      new ObjectMapper(
              JsonFactory.builder().enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION).build())
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);

  private static final ObjectMapper YAML_MAPPER =
      new ObjectMapper(
              YAMLFactory.builder().enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION).build())
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);

  public static TestPlan fromFile(Path path) {

    LOGGER.info("fromFile() - Loading test plan file, path={}", path);
    TestPlanFile planFile;
    try {
      var raw = Files.readString(path);
      var substituted = new StringSubstitutor(System.getenv()).replace(raw);
      planFile = YAML_MAPPER.readValue(substituted, TestPlanFile.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (planFile.targetName() != null && planFile.customTarget != null) {
      throw new RuntimeException(
          "Both targetName and customTarget set, use only one. testPlanFile=" + path);
    }

    var testPlan =
        planFile.customTarget() != null
            ? create(planFile.customTarget(), planFile.workflows, planFile.ignoreDisabled)
            : create(planFile.targetName(), planFile.workflows(), planFile.ignoreDisabled());

    return testPlan;
  }

  public static TestPlan create(String targetName, List<String> workflows) {
    return create(targetName, workflows, true);
  }

  public static TestPlan create(String targetName, List<String> workflows, Boolean ignoreDisabled) {
    var targetConfigs = TargetsSpec.loadAll("testbench/targets/targets.json");
    return create(targetConfigs.configuration(targetName), workflows, ignoreDisabled);
  }

  public static TestPlan create(
          TargetConfiguration targetConfiguration, List<String> workflows, Boolean ignoreDisabled) {
    var target = new Target(targetConfiguration);

    var specFiles =
        SpecFiles.loadAll(
            List.of(
                    "testbench/assertions",
                    "testbench/testsuites",
                    "testbench/workflows"
                    ));

    return new TestPlan(
        target,
        specFiles,
        workflows == null ? Set.of() : Set.copyOf(workflows),
        ignoreDisabled == null || ignoreDisabled);
  }

  public Stream<WorkflowSpec> selectedWorkflows() {

    return specFiles
        .byKind(TestSpecKind.WORKFLOW)
        .filter(
            specFile -> workflows.isEmpty() || workflows.contains(specFile.spec().meta().name()))
        .map(testSpec -> (WorkflowSpec) testSpec.spec());
  }

  public TestPlanNodeTree testNode() {

    var desc =
        "TestPlan: %s on %s"
            .formatted(
                target.configuration().name(),
                target.configuration().backend());

    var uriBuilder =
        TestUri.builder(TestUri.Scheme.DATAAPI)
            .addSegment(TestUri.Segment.TARGET, target.configuration().name());

    var testNodeFactory = new TestNodeFactory(this);

    var root = testNodeFactory.testPlanContainer(
            desc,
            uriBuilder.build().uri(),
            selectedWorkflows()
                .map(workflow -> workflow.testNode(testNodeFactory, uriBuilder.clone(), ignoreDisabled))
                    .toList());
    return new TestPlanNodeTree(root, testNodeFactory.testNodeCount());
  }

  public void updateJobForTarget(Job job) {
    target.updateJobForTarget(job);
  }

  public record TestPlanFile(
      String name,
      String targetName,
      TargetConfiguration customTarget,
      List<String> workflows,
      Boolean ignoreDisabled,
      Map<String, String> envVars) {}

  public record TestPlanNodeTree(DynamicNode root,
                                 int totalNodeCount){}
}
