package io.stargate.sgv2.jsonapi.api.v1.vectorize;

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

    var specFiles = SpecFiles.loadAll("integration-tests/vectorize");

    return new  TestPlan(target, specFiles, Set.copyOf(workflows));
  }

  public Stream<Workflow> selectedWorkflows(){

    return specFiles.byKind(TestSpec.TestSpecKind.WORKFLOW)
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

    return Stream.of(
        dynamicContainer(
            desc,
            selectedWorkflows()
                .map(workflow -> workflow.testNode(this))
        )
    );
  }

  public void updateJobForTarget(Job job){
    target.updateJobForTarget(job);
  }

  private  static Optional<DynamicContainer> containerIfPresent(String namePrefix, TestSpecMeta meta, Optional<DynamicNode> dynamicNode){
    return dynamicNode
        .map(
            node -> dynamicContainer(namePrefix + ": " + meta.name(), Stream.of(node))
        );
  }

  private static Stream<DynamicNode> streamIfPresent(Optional<DynamicContainer> container){
    return container.stream().flatMap(Stream::of);
  }

  private static Stream<DynamicNode> lifecycleNodes(String namePrefix, TestSpecMeta meta, Supplier<Optional<DynamicNode>> nodeSupplier){
    var targetDynamicNode = nodeSupplier.get();
    return streamIfPresent(containerIfPresent(namePrefix, meta, targetDynamicNode));
  }

  public Stream<? extends DynamicNode> addLifecycle(Workflow workflow, Stream<? extends DynamicNode> dynamicNodes){

    return Stream.concat(
        lifecycleNodes("Before Workflow", workflow.meta(), () -> target.beforeWorkflow(this, workflow)),
        Stream.concat(dynamicNodes,
            lifecycleNodes("After Workflow", workflow.meta(), () -> target.afterWorkflow(this, workflow)))
    );
  }

  public Stream<? extends DynamicNode> addLifecycle(Job job, Stream<? extends DynamicNode> dynamicNodes){

    return Stream.concat(
        lifecycleNodes("Before Job", job.meta(), () -> target.beforeJob(this, job)),
        Stream.concat(dynamicNodes,
            lifecycleNodes("After Job", job.meta(), () -> target.afterJob(this, job)))
    );
  }

  public Stream<? extends DynamicNode> addLifecycle(TestSuite testSuite, TestEnvironment environment, Stream<? extends DynamicNode> dynamicNodes){

    return Stream.concat(
        lifecycleNodes("Before TestSuite", testSuite.meta(), () -> target.beforeTestSuite(this, testSuite, environment)),
        Stream.concat(dynamicNodes,
            lifecycleNodes("After TestSuite", testSuite.meta(), () -> target.afterTestSuite(this, testSuite, environment)))
    );
  }
}
