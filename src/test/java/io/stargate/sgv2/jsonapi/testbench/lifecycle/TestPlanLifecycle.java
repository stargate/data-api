package io.stargate.sgv2.jsonapi.testbench.lifecycle;

import io.stargate.sgv2.jsonapi.testbench.testrun.TestNodeFactory;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunEnv;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import io.stargate.sgv2.jsonapi.testbench.testspec.Job;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestSuiteSpec;
import io.stargate.sgv2.jsonapi.testbench.testspec.WorkflowSpec;
import java.util.Optional;
import org.junit.jupiter.api.DynamicNode;

/**
 * Defines the stages in the lifecycle of a test bench run.
 *
 * <p>Design to be implemented by a {@link io.stargate.sgv2.jsonapi.testbench.targets.Backend} so
 * that it can make changes to the data environement so tests can run in a common environement. For
 * example, when we use Cassandra as a backend we need to create a keyspace but for Astra we use the
 * default one.
 *
 * <p>There should not be any test logic within the implementations, that should all be in the test
 * defintoions.
 *
 * <p>Extracted to an interface so it can be reused, such as at the higher level {@link
 * io.stargate.sgv2.jsonapi.testbench.targets.Target} which includes a backend.
 *
 * <p>All methods allow the implementation to return a JUNIT {@link DynamicNode} which will be
 * inserted before or after the dynamic nodes at that level. The returned node could be a single
 * {@link org.junit.jupiter.api.DynamicTest} or a {@link org.junit.jupiter.api.DynamicContainer}.
 */
public interface TestPlanLifecycle {

  /**
   * Called to optionally add a node to execute before the start of the workflow.
   *
   * @param testNodeFactory Factory to use to create test nodes, see {@link TestNodeFactory}
   * @param uriBuilder Builder to use to create the URI's added to dynamic nodes.
   * @param workflow The workflow we are running.
   * @return Optional {@link DynamicNode} to run before the workflow.
   */
  default Optional<DynamicNode> beforeWorkflow(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, WorkflowSpec workflow) {
    return Optional.empty();
  }

  /**
   * Called to optionally add a node to execute after the workflow completes.
   *
   * @param testNodeFactory Factory to use to create test nodes, see {@link TestNodeFactory}
   * @param uriBuilder Builder to use to create the URI's added to dynamic nodes.
   * @param workflow The workflow that just completed.
   * @return Optional {@link DynamicNode} to run after the workflow.
   */
  default Optional<DynamicNode> afterWorkflow(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, WorkflowSpec workflow) {
    return Optional.empty();
  }

  /**
   * Called to optionally add a node to execute before a job starts.
   *
   * @param testNodeFactory Factory to use to create test nodes, see {@link TestNodeFactory}
   * @param uriBuilder Builder to use to create the URI's added to dynamic nodes.
   * @param job The job about to execute.
   * @return Optional {@link DynamicNode} to run before the job.
   */
  default Optional<DynamicNode> beforeJob(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return Optional.empty();
  }

  /**
   * Called to optionally add a node to execute after a job completes.
   *
   * @param testNodeFactory Factory to use to create test nodes, see {@link TestNodeFactory}
   * @param uriBuilder Builder to use to create the URI's added to dynamic nodes.
   * @param job The job that just completed.
   * @return Optional {@link DynamicNode} to run after the job.
   */
  default Optional<DynamicNode> afterJob(
      TestNodeFactory testNodeFactory, TestUri.Builder uriBuilder, Job job) {
    return Optional.empty();
  }

  /**
   * Called to optionally add a node to execute before a test suite runs.
   *
   * @param testNodeFactory Factory to use to create test nodes, see {@link TestNodeFactory}
   * @param uriBuilder Builder to use to create the URI's added to dynamic nodes.
   * @param test The test suite about to execute.
   * @param env The runtime environment for this test, including resolved variables and
   *     configuration.
   * @return Optional {@link DynamicNode} to run before the test suite.
   */
  default Optional<DynamicNode> beforeTestSuite(
      TestNodeFactory testNodeFactory,
      TestUri.Builder uriBuilder,
      TestSuiteSpec test,
      TestRunEnv env) {
    return Optional.empty();
  }

  /**
   * Called to optionally add a node to execute after a test suite completes.
   *
   * @param testNodeFactory Factory to use to create test nodes, see {@link TestNodeFactory}
   * @param uriBuilder Builder to use to create the URI's added to dynamic nodes.
   * @param test The test suite that just completed.
   * @param env The runtime environment for this test, including resolved variables and
   *     configuration.
   * @return Optional {@link DynamicNode} to run after the test suite.
   */
  default Optional<DynamicNode> afterTestSuite(
      TestNodeFactory testNodeFactory,
      TestUri.Builder uriBuilder,
      TestSuiteSpec test,
      TestRunEnv env) {
    return Optional.empty();
  }
}
