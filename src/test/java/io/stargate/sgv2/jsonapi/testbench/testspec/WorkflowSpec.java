package io.stargate.sgv2.jsonapi.testbench.testspec;



import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import java.util.List;

import org.junit.jupiter.api.DynamicNode;

public record WorkflowSpec(TestSpecMeta meta, List<Job> jobs) implements TestSpec {

  public DynamicNode testNode(
          TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, boolean ignoreDisabled) {

    uriBuilder.addSegment(TestUri.Segment.WORKFLOW, meta().name());
    var desc = "Workflow: %s ".formatted(meta.name());

    var testNodeJobs =
        ignoreDisabled
            ? jobs().stream().filter(job -> !job.meta().tags().contains("disabled"))
            : jobs().stream();
    var jobNodes = testNodeJobs
            .map(job -> job.testNode(testNodeFactory, uriBuilder.clone()))
            .toList();

    return testNodeFactory.testPlanContainer(
        desc, uriBuilder.build().uri(), testNodeFactory.addLifecycle(uriBuilder.clone(), this, jobNodes));
  }
}
