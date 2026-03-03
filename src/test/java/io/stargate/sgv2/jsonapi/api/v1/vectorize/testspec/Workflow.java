package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.Job;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;
import org.junit.jupiter.api.DynamicContainer;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

public record Workflow(TestSpecMeta meta, List<Job> jobs) implements TestSpec {


  public Stream<Job> activeJobs(){
    return jobs().stream()
        .filter(job -> !job.meta().tags().contains("disabled"));
  }


  public DynamicContainer testNode(TestPlan testPlan, TestUri.Builder uriBuilder) {

    uriBuilder.addSegment(TestUri.Segment.WORKFLOW, meta().name());
    var desc = "Workflow: %s ".formatted(
        meta.name());

    var jobNodes = activeJobs()
        .map(job -> job.testNode(testPlan, uriBuilder.clone()));

    return dynamicContainer(
            desc,
            uriBuilder.build().uri(),
            testPlan.addLifecycle(uriBuilder.clone(), this,  jobNodes));
  }

}
