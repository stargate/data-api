package io.stargate.sgv2.jsonapi.api.v1.vectorize.reporting;

import static org.apache.maven.surefire.shared.utils.logging.MessageUtils.buffer;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.maven.plugin.surefire.report.Theme;
import org.apache.maven.surefire.shared.utils.logging.MessageBuilder;
import org.junit.platform.engine.TestExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes messages for the output of a Test Bench runs using a LOGGER.
 *
 * <p>Works in two modes, because we need to wait for all the tests to complete so we know what was
 * successful. For info on how the output will look like see {@link Theme} and {@link
 * org.apache.maven.surefire.shared.utils.logging.AnsiMessageBuilder}
 *
 * <p>Call {@link #testStarted(DynamicTreeListener.TestReportingTracker)} when a test is being
 * executed and it has started, will print the progress of the tests. There is no correlating test
 * finished.
 *
 * <p>Call {@link #allTestsFinished(DynamicTreeListener.TestReportingTracker)} with the root
 * tracker for the rest run, this should be called once all the tests have completed so we know what
 * failed and how long things took. This will print the full test tree, but only go down to the
 * request + assertion level for test scenarios that have failed.
 */
public class TestBenchConsoleWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBenchConsoleWriter.class);

  private boolean firstLine = true;

  private final Theme theme;

  public TestBenchConsoleWriter() {
    this(Theme.UNICODE);
  }

  public TestBenchConsoleWriter(Theme theme) {
    this.theme = Objects.requireNonNull(theme, "theme must not be null");
  }

  /**
   * Writes that a test has started, without knowing how it will end, used when the test suite is
   * running.
   *
   * @param tracker Tracker for the test that is running.
   */
  public void testStarted(int totalTestCount, int startedTestCount, DynamicTreeListener.TestReportingTracker tracker) {

    var buffer = buffer();
    if (firstLine) {
      firstLine = false;
      buffer
          .newline()
          .a(theme.dash().repeat(20))
          .newline()
          .a("Running Test Bench, results shown at completion...")
          .newline()
          .a(theme.dash().repeat(20))
          .newline();
    }

    // if there is no parent, this is the first test that is running.
    // display name is the name the TestBench put on the container
    if (tracker.parent() == null) {
      buffer.a(theme.down());
    } else {
      buffer
          .a(theme.blank().repeat(tracker.depth() - 1))
          .a(theme.entry());
    }

    buffer.a(startedTestCount).a(" of ").a(totalTestCount).a(": ");
    buffer.strong(tracker.identifier().getDisplayName());

    LOGGER.info(buffer.toString());
  }

  /**
   * Call when all the tests have finished running, so we can print a summary for test suites that pass, and
   * details for those that fail.
   * @param rootTracker
   */
  public void allTestsFinished(DynamicTreeListener.TestReportingTracker rootTracker) {

    var reportBuffer = buffer();
    writeCompletedSummary(reportBuffer, rootTracker, true, false);
    var testReport = reportBuffer.toString();

    if (LOGGER.isInfoEnabled()){
      var logLineBuffer = buffer();
      logLineBuffer
              .newline()
              .a(theme.dash().repeat(20))
              .newline()
              .a("Test Bench Results")
              .newline()
              .a(theme.dash().repeat(20))
              .newline();
      logLineBuffer.a(testReport);
      LOGGER.info(logLineBuffer.toString());
    }

    var reportFilePath = System.getProperty("test-bench-report-path");
    if (reportFilePath != null) {
      LOGGER.info("Writing report file to: {}", reportFilePath);

      // this is the info for the top node, so peeps know if they want to go down into the report.
      var testPlanNodeDesc = buffer();
      writeTestDesc(testPlanNodeDesc, rootTracker);
      testPlanNodeDesc.newline();

      String failureReport;
      if (rootTracker.stats().failures() == 0){
        failureReport = "No failures";
      } else {
        var failureReportBuffer = buffer();
        writeFailureMessages(failureReportBuffer, rootTracker);
        failureReport = failureReportBuffer.toString();
      }

      // report is a markdown
      // GitHub collapsable sections
      // https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/organizing-information-with-collapsed-sections
      var markdownReport = """
              ## %s
              
              %s
              
              <details>
              
              <summary>Test Bench Summary</summary>
              
              ```
              %s
              ```
              
              </details>
              
              <details>
              
              <summary>Test Bench Failures</summary>
              
              ```
              %s
              ```
              
              </details>
              """.formatted(rootTracker.identifier().getDisplayName(), testPlanNodeDesc.toString(), testReport, failureReport);

        try {
            Files.writeString(Path.of(reportFilePath), markdownReport);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
  }

  /**
   * TestPlan: smoketest-aws-us-east-1 on astra workflows vectorize-header-workflow Workflow:
   * vectorize-header-workflow Job: nvidia-vectorize TestSuite: vectorize-header-auth TestEnv:
   * [MODEL=NV-Embed-QA, PROVIDER=nvidia] RESULTS.... TestEnv: [MODEL=nvidia/nv-embedqa-e5-v5,
   * PROVIDER=nvidia] RESULTS...
   *
   * @param buffer
   * @param tracker
   * @param isRoot
   */
  private void writeCompletedSummary(
      MessageBuilder buffer,
      DynamicTreeListener.TestReportingTracker tracker,
      boolean isRoot,
      boolean parentFailures) {

    // the tree part of the line
    if (isRoot) {
      buffer.a(theme.down());
    } else {
      buffer.a(theme.blank().repeat(tracker.depth() - 1)).a(theme.entry());
    }

    writeTestDesc(buffer, tracker);
    buffer.newline();

    // If we have a TestEnv then we want to write out the summary of results for it, otherwise
    // descend until we get one
    // OF if there are FAILURES then we descend, these are tests that ran but assertion failed.
    // we do not descend for aborted, these are tests that did not run because of previous failure.
    for (var child : tracker.children()){
      var hasFailures = child.stats() != null && child.stats().failures() > 0;
      if (!child.runUri().leafType().descendantOf(TestUri.Segment.ENV) || hasFailures || parentFailures) {
        writeCompletedSummary(buffer, child, false, hasFailures);
      }
    }
  }

  private void writeFailureMessages(MessageBuilder buffer, DynamicTreeListener.TestReportingTracker tracker) {

    // if we have a throwable, write out the tree node test and the error it generated.
    // ignoring the ABORTED
    if (tracker.throwable().isPresent() && (tracker.junitStatus() == TestExecutionResult.Status.FAILED )) {
      writeTestDesc(buffer, tracker);
      buffer.newline();
      buffer.newline();
      buffer.a(tracker.throwable().get().getMessage());
      buffer.newline().a("-----").newline().newline();
    }
    tracker.children().forEach(child -> writeFailureMessages(buffer, child));
  }
  private void writeTestDesc(MessageBuilder buffer, DynamicTreeListener.TestReportingTracker tracker) {

    // Icon for success for failure,
    // if we have stats then this is a container we use that status, otherwise we use the JUNIT execution status
    if (tracker.stats() == null){
      switch (tracker.junitStatus()) {
        case SUCCESSFUL -> buffer.a(theme.successful());
        case FAILED, ABORTED -> buffer.a(theme.failed());
        case null -> {}
      }
    } else {
      if (tracker.stats().failures() ==0 && tracker.stats().aborted() == 0) {
        buffer.a(theme.successful());
      } else {
        buffer.a(theme.failed());
      }
    }

    // The name of the container or test
    buffer.strong(tracker.identifier().getDisplayName());

    // timing info if available
    if (tracker.stats() != null){
      buffer.a(" - %s s".formatted(tracker.stats().elapsedMillis() / 1000));
    }

    // if we have stats write a stats line, these are aggregate for all things below.
    // alternative is to only print them for Test Environment these are lines like
    // TestEnv: [MODEL=text-embedding-3-small, PROVIDER=openai]
    // to do that do this test: (tracker.runUri().leafType() == TestUri.Segment.ENV)

    if (tracker.stats() != null) {
      buffer.a(theme.blank());
      writeTestStats(buffer, tracker);
    }

    // NOTE: does not add new line, caller should
  }

  private void writeTestStats(MessageBuilder buffer, DynamicTreeListener.TestReportingTracker tracker) {
    buffer
            .a("Successful: ").a( tracker.stats().successful()).a(", ")
            .a("Failures: ").a( tracker.stats().failures()).a(", ")
            .a("Aborted: ").a( tracker.stats().aborted());
  }
}
