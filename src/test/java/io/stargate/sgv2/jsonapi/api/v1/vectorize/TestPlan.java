package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;

import java.util.List;
import java.util.Set;
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

  public Stream<? extends DynamicNode> addLifecycle(Workflow workflow, Stream<? extends DynamicNode> dynamicNodes){

    var starting = dynamicTest("Workflow Starting: %s".formatted(workflow.meta().name()), () -> target.workflowStarting(this,workflow));
    var finished = dynamicTest("Workflow Finished: %s".formatted(workflow.meta().name()), () -> target.workflowFinished(this,workflow));

    return Stream.concat(
        Stream.of(starting),
        Stream.concat(dynamicNodes, Stream.of(finished))
    );
  }

  public Stream<? extends DynamicNode> addLifecycle(Job job, Stream<? extends DynamicNode> dynamicNodes){

    var starting = dynamicTest("Job Starting: %s".formatted(job.meta().name()), () -> target.jobStarting(this,job));
    var finished = dynamicTest("Job Finished: %s".formatted(job.meta().name()), () -> target.jobFinished(this,job));

    return Stream.concat(
        Stream.of(starting),
        Stream.concat(dynamicNodes, Stream.of(finished))
    );
  }

  public Stream<? extends DynamicNode> addLifecycle(TestSuite testSuite, TestEnvironment environment, Stream<? extends DynamicNode> dynamicNodes){

    var starting = dynamicTest("TestRun Starting: %s".formatted(testSuite.meta().name()), () -> target.testRunStarting(this,testSuite, environment));
    var finished = dynamicTest("TestRun Finished: %s".formatted(testSuite.meta().name()), () -> target.testRunFinished(this,testSuite, environment));

    return Stream.concat(
        Stream.of(starting),
        Stream.concat(dynamicNodes, Stream.of(finished))
    );
  }
}
