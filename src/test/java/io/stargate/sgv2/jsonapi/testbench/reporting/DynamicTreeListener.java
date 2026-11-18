package io.stargate.sgv2.jsonapi.testbench.reporting;

import io.stargate.sgv2.jsonapi.testbench.TestBenchPlan;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.UriSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * Listens to the execution of the dynammic tests created by the {@link TestPlan} and logs the
 * results using the {@link TestBenchConsoleWriter}.
 *
 * <p>The class mus tbe registeded by a text file at <code>
 * META-INF/services/org.junit.platform.launcher.TestExecutionListener</code> that contains the
 * fully qualitified path to the class.
 *
 * <p>Type types of output are generated:
 *
 * <ul>
 *   <li>As the tests are running the name of every test node is outputted together with progress,
 *       so we can see how long there is to go. See {@link TestBenchConsoleWriter#testStarted(int,
 *       int, TestReportingTracker)}. At this point we do not know how long child nodes will take to
 *       process and what their result will be.
 *   <li>Once complet a summary is outputted that does not include every node to brevity, see {@link
 *       TestBenchConsoleWriter#allTestsFinished(TestReportingTracker)}. At this point we know how
 *       long child nodes took to process and their result.
 * </ul>
 */
public class DynamicTreeListener implements TestExecutionListener {

  private Integer totalTestCount = null;
  private int startedTestCount = 0;

  private TestReportingTracker rootTracker;
  // Keyed on TestIdentifier.uniqueID() see {@link TestIdentifier#uniqueId()}
  private final Map<UniqueId, TestReportingTracker> testTrackers = new ConcurrentHashMap<>();

  private final TestBenchConsoleWriter writer = new TestBenchConsoleWriter();

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    // All done, write the summary.
    writer.allTestsFinished(rootTracker);
  }

  @Override
  public void executionStarted(TestIdentifier id) {
    if (!isTestBenchNode(id)) {
      return;
    }

    var tracker = getCreateTestTracker(id);
    if (tracker == null) {
      return;
    }

    // Test count will not be in the system properties until we see the first dymamic test node we
    // create, e.g.
    // "TestPlan: smoketest-aws-us-east-1 on astra workflows vectorize-header-workflow"
    // because the nodes will not have been created until then.
    if (totalTestCount == null) {
      totalTestCount =
          Integer.parseInt(System.getProperty(TestBenchPlan.TEST_PLAN_TEST_COUNT_PROPERTY, "0"));
    }

    writer.testStarted(totalTestCount, ++startedTestCount, tracker);
  }

  @Override
  public void executionFinished(TestIdentifier id, TestExecutionResult result) {
    var tracker = getCreateTestTracker(id);
    if (tracker == null) {
      return;
    }

    tracker.executionFinished(result);
  }

  @Override
  public void executionSkipped(TestIdentifier id, String reason) {
    // Looks like we never get skipped, included for completeness.
    var tracker = getCreateTestTracker(id);
    if (tracker == null) {
      return;
    }

    tracker.executionSkipped();
  }

  /** Determine if tests are running that we should be tracking. */
  private static boolean isTestBenchNode(TestIdentifier testIdentifier) {

    // This is the uniqueID created by jupiter as it is traversing the code, once we get to the
    // nodes that are created by the test plan that have different formatting
    return testIdentifier
        .getUniqueId()
        .startsWith("[engine:junit-jupiter]/[class:io.stargate.sgv2.jsonapi.testbench.");
  }

  /**
   * We use a Tracker for every node in the test plan, to track the execution time and result of it
   * and all of its children.
   */
  private TestReportingTracker getCreateTestTracker(TestIdentifier testIdentifier) {

    var existingTracker = testTrackers.get(testIdentifier.getUniqueIdObject());
    if (existingTracker != null) {
      return existingTracker;
    }

    // The getSource() is a URI, jupiter / junit use it to identify the test file, but we dont have
    // those.
    // We use the {@link TestUri} instead. We need one to know what sort of test node this is
    var testUri =
        testIdentifier
            .getSource()
            .map(
                testSource -> {
                  if (testSource instanceof UriSource uriSource) {
                    return TestUri.parse(uriSource.getUri()).orElse(null);
                  }
                  return null;
                });
    if (testUri.isEmpty()) {
      return null;
    }

    // The TARGET is the top level item, everything else should have a parent
    var parentTracker =
        testUri.get().leafType() != TestUri.Segment.TARGET
            ? Objects.requireNonNull(
                testTrackers.get(testIdentifier.getParentIdObject().get()),
                "parentID not found for testIdentifier: " + testIdentifier)
            : null;

    var tracker = new TestReportingTracker(testIdentifier, testUri.get(), parentTracker);

    if (rootTracker == null) {
      rootTracker = tracker;
    }
    testTrackers.put(tracker.identifier.getUniqueIdObject(), tracker);
    return tracker;
  }

  /**
   * Container for tracking the execution of a test, and all of its children.
   *
   * <p>---
   */
  public class TestReportingTracker {

    private final TestIdentifier identifier;
    private final TestUri runUri;
    private final TestReportingTracker parent;
    private final int depth;
    private final TestContainerStats stats;

    private final List<TestReportingTracker> children = new ArrayList<>();

    // Set when we know the test completed, value on the test result is an optional
    private Optional<Throwable> throwable = Optional.empty();
    // Set when we know the test completed
    private TestExecutionResult.Status junitStatus;

    public TestReportingTracker(
        TestIdentifier identifier, TestUri runUri, TestReportingTracker parent) {

      this.identifier = identifier;
      this.runUri = runUri;
      this.parent = parent;
      this.depth = parent == null ? 0 : parent.depth + 1;

      this.stats = identifier.isContainer() ? new TestContainerStats() : null;
      if (parent != null) {
        parent.children.add(this);
      }
    }

    public Optional<Throwable> throwable() {
      return throwable;
    }

    public TestExecutionResult.Status junitStatus() {
      return junitStatus;
    }

    public TestIdentifier identifier() {
      return identifier;
    }

    public TestUri runUri() {
      return runUri;
    }

    public TestReportingTracker parent() {
      return parent;
    }

    public List<TestReportingTracker> children() {
      return Collections.unmodifiableList(children);
    }

    public int depth() {
      return depth;
    }

    public TestContainerStats stats() {
      return stats;
    }

    /**
     * Call when the execution of the test is finished, updates tracking for the node and for any
     * ancestors.
     *
     * @param result
     */
    public void executionFinished(TestExecutionResult result) {
      junitStatus = result.getStatus();
      throwable = result.getThrowable();

      if (stats != null) {
        stats.testCompleted(this, result);
      }
      if (parent != null) {
        parent.descendantExecutionFinished(this, result);
      }
    }

    private void descendantExecutionFinished(
        TestReportingTracker originalTracker, TestExecutionResult result) {
      if (stats != null) {
        stats.testCompleted(originalTracker, result);
      }
      if (parent != null) {
        parent.descendantExecutionFinished(originalTracker, result);
      }
    }

    public void executionSkipped() {
      if (stats != null) {
        stats.testSkipped();
      }
      if (parent != null) {
        parent.executionSkipped();
      }
    }
  }

  /**
   * Count the tests that failed etc.
   *
   * <p>Modeled on org.apache.maven.plugin.surefire.report.TestSetStats
   */
  public class TestContainerStats {

    private final long startedAtMillis;
    private long selfOrDescendantFinishedAtMillis;

    private int successful;

    // Aborted happens when the test node decided not to run, normally by calling
    // Assumptions.assumeTrue() which throws a TestAbortedException which is tracked
    // differently by junit.
    private int aborted;

    // An actual failure of an assertion or unexpected error thrown
    private int failures;

    // dont think used, kept for completeness
    private int skipped;

    public TestContainerStats() {
      this.startedAtMillis = System.currentTimeMillis();
    }

    public long elapsedMillis() {
      return selfOrDescendantFinishedAtMillis == 0
          ? System.currentTimeMillis() - startedAtMillis
          : selfOrDescendantFinishedAtMillis - startedAtMillis;
    }

    public int successful() {
      return successful;
    }

    public int aborted() {
      return aborted;
    }

    public int failures() {
      return failures;
    }

    public int skipped() {
      return skipped;
    }

    public void testCompleted(TestReportingTracker tracker, TestExecutionResult result) {
      selfOrDescendantFinishedAtMillis = System.currentTimeMillis();

      // we only update the stats IF the test we are tracking is a TEST, we do not update for
      // containers.
      if (tracker.identifier().isTest()) {
        switch (result.getStatus()) {
          case FAILED -> failures++;
          case ABORTED -> aborted++;
          case SUCCESSFUL -> successful++;
        }
      }
    }

    public void testSkipped() {
      selfOrDescendantFinishedAtMillis = System.currentTimeMillis();
      skipped++;
    }
  }
}
