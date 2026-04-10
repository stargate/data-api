package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.targets.Target;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.*;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

public record TestPlan(Target target, SpecFiles specFiles, Set<String> workflows, boolean ignoreDisabled){

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper(JsonFactory.builder()
          .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
          .build()
      )
      .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
          YAMLFactory.builder()
                  .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
                  .build()
          )
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);

  public static TestPlanContext fromFile(String path) {

    TestPlanFile planFile;
    try {
      planFile =  YAML_MAPPER.readValue(Path.of(path).toFile(), TestPlanFile.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (planFile.targetName() != null && planFile.customTarget!= null){
      throw new RuntimeException("Both targetName and customTarget set, use only one. testPlanFile=" + path);
    }

    var testPlan = planFile.customTarget() != null ?
            create(planFile.customTarget(), planFile.workflows, planFile.ignoreDisabled)
            :
            create(planFile.targetName(), planFile.workflows(), planFile.ignoreDisabled());

    return new TestPlanContext(testPlan, planFile);
  }
  public static TestPlan  create(String targetName, List<String> workflows){
    return create(targetName, workflows, true);
  }

  public static TestPlan  create(String targetName, List<String> workflows, Boolean ignoreDisabled){
    var targetConfigs = TargetsSpec.loadAll("integration-tests/targets/targets.json");
    return create(targetConfigs.configuration(targetName), workflows, ignoreDisabled);
  }

  public static TestPlan  create(TargetConfiguration targetConfiguration, List<String> workflows, Boolean ignoreDisabled){
    var target = new Target(targetConfiguration);

    var specFiles = SpecFiles.loadAll(List.of("integration-tests/vectorize", "integration-tests/assertions/assertion-templates.json"));

    return new  TestPlan(
            target,
            specFiles,
            workflows == null ? Set.of() :  Set.copyOf(workflows),
            ignoreDisabled == null || ignoreDisabled);
  }

  public Stream<WorkflowSpec> selectedWorkflows(){

    return specFiles.byKind(TestSpecKind.WORKFLOW)
        .filter(specFile ->
          workflows.isEmpty() || workflows.contains(specFile.spec().meta().name())
        )
        .map(testSpec -> (WorkflowSpec) testSpec.spec());
  }

  public Stream<DynamicContainer> testNode() {

    var desc = "TestPlan: %s on %s workflows %s".formatted(
        target.configuration().name(),
        target.configuration().backend(),
        workflows.isEmpty() ? "<all>" : String.join(", ", workflows)
    );

    var uriBuilder = TestUri.builder(TestUri.Scheme.TESTRUN)
        .addSegment(TestUri.Segment.TARGET, target.configuration().name());

    return Stream.of(
        dynamicContainer(
            desc,
            uriBuilder.build().uri(),
            selectedWorkflows()
                .map(workflow -> workflow.testNode(this, uriBuilder.clone(), ignoreDisabled))
        )
    );
  }

  public void updateJobForTarget(Job job){
    target.updateJobForTarget(job);
  }

  private  static Optional<DynamicContainer> containerIfPresent(TestUri.Builder uriBuilder, String namePrefix, TestSpecMeta meta, Optional<DynamicNode> dynamicNode){
    return dynamicNode
        .map(
            node -> dynamicContainer(namePrefix + ": " + meta.name(), uriBuilder.build().uri(), Stream.of(node))
        );
  }

  private static Stream<DynamicNode> streamIfPresent(Optional<DynamicContainer> container){
    return container.stream().flatMap(Stream::of);
  }

  private static Stream<DynamicNode> lifecycleNodes(TestUri.Builder uriBuilder, String namePrefix, TestSpecMeta meta, Supplier<Optional<DynamicNode>> nodeSupplier){
    var targetDynamicNode = nodeSupplier.get();
    return streamIfPresent(containerIfPresent(uriBuilder, namePrefix, meta, targetDynamicNode));
  }

  public Stream<? extends DynamicNode> addLifecycle(TestUri.Builder uriBuilder, WorkflowSpec workflow, Stream<? extends DynamicNode> dynamicNodes){

    var beforeUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "before-workflow");
    var afterUriBuilder =  uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "after-workflow");

    return Stream.concat(
        lifecycleNodes(beforeUriBuilder, "Before Workflow", workflow.meta(), () -> target.beforeWorkflow(this,beforeUriBuilder, workflow)),
        Stream.concat(dynamicNodes,
            lifecycleNodes(afterUriBuilder, "After Workflow", workflow.meta(), () -> target.afterWorkflow(this, afterUriBuilder, workflow)))
    );
  }

  public Stream<? extends DynamicNode> addLifecycle(TestUri.Builder uriBuilder, Job job, Stream<? extends DynamicNode> dynamicNodes){

    var beforeUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "before-job");
    var afterUriBuilder =  uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "after-job");

    return Stream.concat(
        lifecycleNodes(beforeUriBuilder, "Before Job", job.meta(), () -> target.beforeJob(this,beforeUriBuilder, job)),
        Stream.concat(dynamicNodes,
            lifecycleNodes(afterUriBuilder, "After Job", job.meta(), () -> target.afterJob(this,afterUriBuilder, job)))
    );
  }

  public Stream<? extends DynamicNode> addLifecycle(TestUri.Builder uriBuilder, TestSuiteSpec testSuite, TestRunEnv environment, Stream<? extends DynamicNode> dynamicNodes){

    var beforeUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "before-test-suite");
    var afterUriBuilder =  uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "after-test-suite");

    return Stream.concat(
        lifecycleNodes(beforeUriBuilder, "Before TestSuite", testSuite.meta(), () -> target.beforeTestSuite(this, beforeUriBuilder,testSuite, environment)),
        Stream.concat(dynamicNodes,
            lifecycleNodes( afterUriBuilder, "After TestSuite", testSuite.meta(), () -> target.afterTestSuite(this, afterUriBuilder,testSuite, environment)))
    );
  }

  public record TestPlanFile(
      String name,
      String targetName,
      TargetConfiguration customTarget,
      List<String> workflows,
      Boolean ignoreDisabled,
      Map<String, String> envVars
  ) { }

  public record TestPlanContext(
      TestPlan testPlan,
      TestPlanFile testPlanFile
  ) implements AutoCloseable {

    public TestPlanContext{
      testPlanFile.envVars.forEach(System::setProperty);
    }

    @Override
    public void close() {
      testPlanFile.envVars.keySet().forEach(System::clearProperty);
    }
  }
}
