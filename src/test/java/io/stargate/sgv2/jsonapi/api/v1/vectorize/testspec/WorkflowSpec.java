package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestPlan;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import java.util.List;
import org.junit.jupiter.api.DynamicContainer;

public record WorkflowSpec(TestSpecMeta meta, List<Job> jobs) implements TestSpec {

  public DynamicContainer testNode(TestPlan testPlan, TestUri.Builder uriBuilder) {
    return testNode(testPlan, uriBuilder, true);
  }

  public DynamicContainer testNode(
      TestPlan testPlan, TestUri.Builder uriBuilder, boolean ignoreDisabled) {

    uriBuilder.addSegment(TestUri.Segment.WORKFLOW, meta().name());
    var desc = "Workflow: %s ".formatted(meta.name());

    var testNodeJobs =
        ignoreDisabled
            ? jobs().stream().filter(job -> !job.meta().tags().contains("disabled"))
            : jobs().stream();
    var jobNodes = testNodeJobs.map(job -> job.testNode(testPlan, uriBuilder.clone()));

    return dynamicContainer(
        desc, uriBuilder.build().uri(), testPlan.addLifecycle(uriBuilder.clone(), this, jobNodes));
  }
}
