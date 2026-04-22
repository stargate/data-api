package io.stargate.sgv2.jsonapi.api.v1.vectorize.reporting;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.maven.plugin.surefire.report.*;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.UriSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class DynamicTreeListener implements TestExecutionListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicTreeListener.class);

  private TestReportingTracker rootTracker;
  private final Map<UniqueId, TestReportingTracker> testTrackers = new ConcurrentHashMap<>();

  private final Map<String, Long> startTimes = new ConcurrentHashMap<>();
  private final Map<String, TestSetStats> containerStats = new ConcurrentHashMap<>();

  private final TestBenchConsoleWriter writer = new TestBenchConsoleWriter();

  @Override
  public void testPlanExecutionStarted(TestPlan testPlan) {}

  @Override
  public void testPlanExecutionFinished(TestPlan testPlan) {
    writer.allTestsFinished(rootTracker);
  }

  @Override
  public void dynamicTestRegistered(TestIdentifier testIdentifier) {}

  @Override
  public void executionStarted(TestIdentifier id) {
    if (!isTestBenchNode(id)) {
      return;
    }
    var tracker = getCreateTestTracker(id);
    if (tracker == null) {
      return;
    }
    writer.testStarted(tracker);
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
    var tracker = getCreateTestTracker(id);
    if (tracker == null) {
      return;
    }

    tracker.executionSkipped();
  }

  private static boolean isTestBenchNode(TestIdentifier testIdentifier) {
    return testIdentifier
        .getUniqueId()
        .startsWith("[engine:junit-jupiter]/[class:io.stargate.sgv2.jsonapi.");
  }

  //    private String buildClassName(TestIdentifier id) {
  //        List<String> parts = new ArrayList<>();
  //        Optional<TestIdentifier> current = Optional.of(id);
  //        while (current.isPresent()) {
  //            TestIdentifier node = current.get();
  //            if (isEngineOrClass(node)) {
  //                break;
  //            }
  //            parts.add(0, node.getDisplayName());
  //            current = testPlan.getParent(node);
  //        }
  //        return rootClassName(id) + (parts.isEmpty() ? "" : "$" + String.join("$", parts));
  //    }
  //
  //    private String rootClassName(TestIdentifier id) {
  //        Optional<TestIdentifier> current = Optional.of(id);
  //        while (current.isPresent()) {
  //            TestIdentifier node = current.get();
  //            if (isClassNode(node)) {
  //                return node.getDisplayName();
  //            }
  //            current = testPlan.getParent(node);
  //        }
  //        return "Unknown";
  //    }
  //
  //    private SimpleReportEntry toReportEntry(TestIdentifier id, String methodName, String
  // methodDisplay) {
  //        return new SimpleReportEntry(
  //                RunMode.NORMAL_RUN,
  //                System.currentTimeMillis(),
  //                buildClassName(id),
  //                id.getDisplayName(),
  //                methodName,
  //                methodDisplay
  //        );
  //    }
  //
  //    private boolean isEngineOrClass(TestIdentifier id) {
  //        return id.getUniqueId().startsWith("[engine:") || isClassNode(id);
  //    }
  //
  //    private boolean isClassNode(TestIdentifier id) {
  //        return id.getUniqueId().contains("[class:");
  //    }
  //
  //    private long elapsed(TestIdentifier id) {
  //        return System.currentTimeMillis() - startTimes.getOrDefault(id.getUniqueId(),
  // System.currentTimeMillis());
  //    }
  //
  //    private Optional<TestSetStats> nearestContainerStats(TestIdentifier id) {
  //        Optional<TestIdentifier> current = testPlan.getParent(id);
  //        while (current.isPresent()) {
  //            if (containerStats.containsKey(current.get().getUniqueId())) {
  //                return Optional.of(containerStats.get(current.get().getUniqueId()));
  //            }
  //            current = testPlan.getParent(current.get());
  //        }
  //        return Optional.empty();
  //    }

  private TestReportingTracker getCreateTestTracker(TestIdentifier testIdentifier) {

    var existingTracker = testTrackers.get(testIdentifier.getUniqueIdObject());
    if (existingTracker != null) {
      return existingTracker;
    }

    // if this is not a TESTRUN:// it is not a node we care about
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
                "parentID not found for testIdentifier: " + testIdentifier.toString())
            : null;

    var tracker = new TestReportingTracker(testIdentifier, testUri.get(), parentTracker);

    if (rootTracker == null) {
      rootTracker = tracker;
    }
    testTrackers.put(tracker.identifier.getUniqueIdObject(), tracker);
    return tracker;
  }

  /** */
  public class TestReportingTracker {

    private final TestIdentifier identifier;
    private final TestUri runUri;
    private final TestReportingTracker parent;
    private final int depth;

    private final List<TestReportingTracker> children = new ArrayList<>();

    private Optional<Throwable> throwable = Optional.empty();
    private TestExecutionResult.Status junitStatus;
    private final TestContainerStats stats;

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

    @Nullable
    public TestExecutionResult.Status junitStatus(){
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
      return children;
    }

    public int depth() {
      return depth;
    }

    public TestContainerStats stats() {
      return stats;
    }

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

    private void descendantExecutionFinished(TestReportingTracker originalTracker,  TestExecutionResult result) {
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

  /** Modeled on org.apache.maven.plugin.surefire.report.TestSetStats */
  public class TestContainerStats {

    private TestContainerStats parent;

    private final long startedAtMillis;
    private long lastFinishedAtMillis;

    private int successful;

    private int aborted;

    private int failures;

    private int skipped;

    public TestContainerStats() {
      this.startedAtMillis = System.currentTimeMillis();
    }

    public long elapsedMillis() {
      return lastFinishedAtMillis == 0
          ? System.currentTimeMillis() - startedAtMillis
          : lastFinishedAtMillis - startedAtMillis;
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

//    public boolean noErrors() {
//      return aborted == 0 && failures == 0;
//    }

    public void testCompleted(TestReportingTracker tracker,  TestExecutionResult result) {
      lastFinishedAtMillis = System.currentTimeMillis();

      // we only update the stats IF the test we are tracking is a TEST, we do not update for containers.
      if (tracker.identifier().isTest()) {
        switch (result.getStatus()) {
          case FAILED -> failures++;
          case ABORTED -> aborted++;
          case SUCCESSFUL -> successful++;
        }
      }
    }

    public void testSkipped() {
      lastFinishedAtMillis = System.currentTimeMillis();
      skipped++;
    }
  }
}
