package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import org.junit.jupiter.api.DynamicContainer;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

public record Workflow(TestSpecMeta meta, List<Job> jobs) implements TestSpec {

  @Override
  public TestSpecKind kind() {
    return TestSpecKind.WORKFLOW;
  }

  public Stream<Job> activeJobs(){
    return jobs().stream()
        .filter(job -> !job.meta().tags().contains("disabled"));
  }


  public DynamicContainer testNode(TestPlan testPlan) {

    var desc = "Workflow: %s ".formatted(
        meta.name());

    var jobNodes = activeJobs()
        .map(job -> job.testNode(testPlan));

    return dynamicContainer(
            desc,
            testPlan.addLifecycle(this, jobNodes));
  }

}
