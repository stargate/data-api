package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec.*;
import org.eclipse.aether.util.artifact.OverlayArtifactTypeRegistry;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public record TestPlan(Target target, SpecFiles specFiles, Set<String> workflows){

  public static TestPlan  create(String targetName, List<String> workflows){
    var targetConfigs = TargetConfigurationss.loadAll("integration-tests/targets/targets.json");
    var target = new Target(targetConfigs.configuration(targetName));

    var specFiles = SpecFiles.loadAll(List.of("integration-tests/vectorize", "integration-tests/assertions/assertion-templates.json"));

    return new  TestPlan(target, specFiles, Set.copyOf(workflows));
  }

  public Stream<Workflow> selectedWorkflows(){

    return specFiles.byKind(TestSpecKind.WORKFLOW)
        .filter(specFile ->
          workflows.isEmpty() || workflows.contains(specFile.spec().meta().name())
        )
        .map(testSpec -> (Workflow) testSpec.spec());
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
                .map(workflow -> workflow.testNode(this, uriBuilder.clone()))
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

  public Stream<? extends DynamicNode> addLifecycle(TestUri.Builder uriBuilder, Workflow workflow, Stream<? extends DynamicNode> dynamicNodes){

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

  public Stream<? extends DynamicNode> addLifecycle(TestUri.Builder uriBuilder, TestSuite testSuite, TestEnvironment environment, Stream<? extends DynamicNode> dynamicNodes){

    var beforeUriBuilder = uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "before-test-suite");
    var afterUriBuilder =  uriBuilder.clone().addSegment(TestUri.Segment.LIFECYCLE, "after-test-suite");

    return Stream.concat(
        lifecycleNodes(beforeUriBuilder, "Before TestSuite", testSuite.meta(), () -> target.beforeTestSuite(this, beforeUriBuilder,testSuite, environment)),
        Stream.concat(dynamicNodes,
            lifecycleNodes( afterUriBuilder, "After TestSuite", testSuite.meta(), () -> target.afterTestSuite(this, afterUriBuilder,testSuite, environment)))
    );
  }
}
