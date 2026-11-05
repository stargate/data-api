package io.stargate.sgv2.jsonapi.testbench.reporting;

import static org.apache.maven.surefire.shared.utils.logging.MessageUtils.buffer;

import io.stargate.sgv2.jsonapi.testbench.testrun.TestUri;
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
 * Writes messages for the output of Test Bench using the logging system.
 *
 * <p>NOTE: to get the best output via the logging system, we need to configure to remove the normal
 * formatting that comes with logging. Below should be in the `applicaiton.yaml` in addition to the
 * regular config
 *
 * <pre>
 * quarkus:
 *   log:
 *     console:
 *       format: "%-5p [%t] %d{yyyy-MM-dd HH:mm:ss,SSS} %F:%L - %m%n"
 *     handler:
 *       console:
 *         PLAIN_CONSOLE:
 *           format: "%m%n"
 *     category:
 *       'io.stargate.sgv2.jsonapi.testbench.reporting.TestBenchConsoleWriter':
 *         level: INFO
 *         handlers:
 *           - PLAIN_CONSOLE
 *         use-parent-handlers: false
 * </pre>
 *
 * <p>Works in two modes because we need to wait for all the tests to complete so we know what was
 * successful. For info on how the output will look see {@link Theme} and {@link
 * org.apache.maven.surefire.shared.utils.logging.AnsiMessageBuilder}
 *
 * <p>Call {@link #testStarted(int, int, DynamicTreeListener.TestReportingTracker)} when a test
 * starts executing, will print the progress of the tests
 *
 * <p>Call {@link #allTestsFinished(DynamicTreeListener.TestReportingTracker)} with the root tracker
 * for the rest run, this should be called once all the tests have completed so we know what failed
 * and how long things took. This will print the full test tree, but only go down to the request +
 * assertion level for test scenarios that have failed.
 */
public class TestBenchConsoleWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBenchConsoleWriter.class);

  public static final String ENV_TEST_BENCH_REPORT_PATH = "test-bench-report-path";

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
   * <p>Example output:
   *
   * <pre>
   * ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
   * Running Test Bench, results shown at completion...
   * ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
   *  ┬─ 1 of 1465: TestPlan: smoketest-prod-aws-ap-south-1 (701031671627) on astra (node:axM)
   * ├─ 2 of 1465: Workflow: vectorize-shared-workflow  (node:alW)
   *    ├─ 3 of 1465: Job: open-ai-vectorize  (node:ab7)
   *       ├─ 4 of 1465: TestSuite: vectorize-shared-auth  (node:ab6)
   *          ├─ 5 of 1465: TestEnv: [CREDENTIAL=open-ai-key, MODEL=text-embedding-3-small, PROVIDER=openai]  (node:aaN)
   *             ├─ 6 of 1465: Request: SetupRequest[1]: CREATE_COLLECTION (node:aaf)
   *                ├─ 7 of 1465: Command: createCollection (node:aaa)
   *  </pre>
   *
   * @param totalTestCount the total number of tests that will be run
   * @param startedTestCount the number of tests that have started running, including this one
   * @param tracker the tracker for the test that started
   */
  public void testStarted(
      int totalTestCount, int startedTestCount, DynamicTreeListener.TestReportingTracker tracker) {

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

    writeTreeOutline(buffer, tracker);

    // the progress "x of Y" and the displayName from the node set when it was created by the
    // TestPlan.
    // NOTE: the "(node:aaN)" is part of the displayName, it is so that when we print failed nodes
    // we can
    // find them in the full log easily.
    buffer
        .a(startedTestCount)
        .a(" of ")
        .a(totalTestCount)
        .a(": ")
        .strong(tracker.identifier().getDisplayName());

    LOGGER.info(buffer.toString());
  }

  /**
   * Call when all the tests have finished running, so we can print a summary for test suites that
   * pass and details for those that fail.
   *
   * <p>Writes two types of output:
   *
   * <ul>
   *   <li>If {@link LOGGER#isInfoEnabled()} logs a detailed report of the test results, skips
   *       details of assertions that have completed successfully. See {@link
   *       #writeCompletedSummary(MessageBuilder, DynamicTreeListener.TestReportingTracker,
   *       boolean)}
   *   <li>If {@link #ENV_TEST_BENCH_REPORT_PATH} is set in the path, writes a markdown file in a
   *       format to be included in the summary for GitHub actions via the <code>
   *       $GITHUB_STEP_SUMMARY</code>.
   * </ul>
   *
   * @param rootTracker Tracker for the root of the test tree, this is the one that has the full
   *     test tree under it.
   */
  public void allTestsFinished(DynamicTreeListener.TestReportingTracker rootTracker) {

    var reportBuffer = buffer();
    writeCompletedSummary(reportBuffer, rootTracker, false);
    var testReport = reportBuffer.toString();

    if (LOGGER.isInfoEnabled()) {

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

    var reportFilePath = System.getProperty(ENV_TEST_BENCH_REPORT_PATH);
    if (reportFilePath != null) {
      LOGGER.info("Writing report file to: {}", reportFilePath);

      // this is the info for the top node, so peeps know if they want to go down into the report.
      var testPlanNodeDesc = buffer();
      writeTestDesc(testPlanNodeDesc, rootTracker);
      testPlanNodeDesc.newline();

      // the failed nodes and their throwable, if any
      String failureReport;
      if (rootTracker.stats().failures() == 0) {
        failureReport = "No failures";
      } else {
        var failureReportBuffer = buffer();
        writeFailureMessages(failureReportBuffer, rootTracker);
        failureReport = failureReportBuffer.toString();
      }

      // report is a markdown file
      // Using GitHub collapsable sections
      // https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/organizing-information-with-collapsed-sections
      var markdownReport =
              """
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
              """
              .formatted(
                  rootTracker.identifier().getDisplayName(),
                  testPlanNodeDesc.toString(),
                  testReport,
                  failureReport);

      try {
        Files.writeString(Path.of(reportFilePath), markdownReport);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Walks the test tree, outputs the results of running each node now that we know the completed
   * results.
   *
   * <p>Example (the header is put in th by the caller) :
   *
   * <pre>
   * ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
   * Test Bench Results
   * ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
   * ┬─ ✔ TestPlan: smoketest-prod-aws-ap-south-1 (701031671627) on astra (node:axM) - 835 s   Successful: 756, Failures: 0, Aborted: 0
   * ├─ ✔ Workflow: vectorize-shared-workflow  (node:alW) - 417 s   Successful: 378, Failures: 0, Aborted: 0
   *    ├─ ✔ Job: open-ai-vectorize  (node:ab7) - 76 s   Successful: 63, Failures: 0, Aborted: 0
   *       ├─ ✔ TestSuite: vectorize-shared-auth  (node:ab6) - 76 s   Successful: 63, Failures: 0, Aborted: 0
   *          ├─ ✔ TestEnv: [CREDENTIAL=open-ai-key, MODEL=text-embedding-3-small, PROVIDER=openai]  (node:aaN) - 30 s   Successful: 21, Failures: 0, Aborted: 0
   *          ├─ ✔ TestEnv: [CREDENTIAL=open-ai-key, MODEL=text-embedding-3-large, PROVIDER=openai]  (node:abr) - 23 s   Successful: 21, Failures: 0, Aborted: 0
   *          ├─ ✔ TestEnv: [CREDENTIAL=open-ai-key, MODEL=text-embedding-ada-002, PROVIDER=openai]  (node:ab5) - 22 s   Successful: 21, Failures: 0, Aborted: 0
   * </pre>
   *
   * <p><b>NOTE:</b> Because there can be 1,000s of test nodes, we do not below the TestEnv nodes
   * unless there is a failure. Below those nodes are where the assertions that will have failed
   * exist. We traverse down to the first failed test node, and then want to show which sibling (or
   * cousin) nodes aborted because of the failure. For example:
   *
   * <pre>
   * ┬─ ✘ TestPlan: smoketest-prod-aws-eu-central-1 (105817733955) on astra (node:axM) - 413 s   Successful: 436, Failures: 20, Aborted: 300
   * ├─ ✘ Workflow: vectorize-shared-workflow  (node:alW) - 41 s   Successful: 90, Failures: 18, Aborted: 270
   *    ├─ ✘ Job: open-ai-vectorize  (node:ab7) - 10 s   Successful: 15, Failures: 3, Aborted: 45
   *       ├─ ✘ TestSuite: vectorize-shared-auth  (node:ab6) - 10 s   Successful: 15, Failures: 3, Aborted: 45
   *          ├─ ✘ TestEnv: [CREDENTIAL=open-ai-key, MODEL=text-embedding-3-small, PROVIDER=openai]  (node:aaN) - 5 s   Successful: 5, Failures: 1, Aborted: 15
   *             ├─ ✘ Request: SetupRequest[1]: CREATE_COLLECTION (node:aaf) - 4 s   Successful: 2, Failures: 1, Aborted: 0
   *                ├─ ✔ Command: createCollection (node:aaa)
   *                ├─ ✘ Assertions (node:aae) - 0 s   Successful: 1, Failures: 1, Aborted: 0
   *                   ├─ ✘ isSuccess (node:aad) - 0 s   Successful: 1, Failures: 1, Aborted: 0
   *                      ├─ ✔ success [http status is 200] (node:aab)
   *                      ├─ ✘ isDDLSuccess [body('$') - responseIsDDLSuccess: REQUIRED:[status], OPTIONAL:[], FORBIDDEN:[data, errors]] (node:aac)
   *             ├─ ✘ Request: SetupRequest[2]: INSERT_ONE (node:aal) - 0 s   Successful: 0, Failures: 0, Aborted: 3
   *             ├─ ✘ Request: SetupRequest[3]: INSERT_MANY (node:aar) - 0 s   Successful: 0, Failures: 0, Aborted: 3
   *             ├─ ✘ Request: TestCase: name=basic findMany (node:aay) - 0 s   Successful: 0, Failures: 0, Aborted: 4
   *             ├─ ✘ Request: TestCase: name=findOneAndUpdate (node:aaG) - 0 s   Successful: 0, Failures: 0, Aborted: 5
   *             ├─ ✔ Request: CleanupRequest[4]: DELETE_COLLECTION (node:aaM) - 0 s   Successful: 3, Failures: 0, Aborted: 0
   * </pre>
   *
   * In the first example we do not go down to the Request nodes because they did not fail or abort.
   *
   * @param buffer Buffer to append the message to.
   * @param tracker The tracker we are writing out the summary for, and may then traverse its
   *     children.
   * @param parentFailures Set true if any parent nodes have failures, this will cause us to
   *     traverse down to the children
   */
  private void writeCompletedSummary(
      MessageBuilder buffer,
      DynamicTreeListener.TestReportingTracker tracker,
      boolean parentFailures) {

    writeTreeOutline(buffer, tracker);
    writeTestDesc(buffer, tracker);
    buffer.newline();

    // If we have a TestEnv then we want to write out the summary of results for it, otherwise
    // descend until we get one
    // OR if there are FAILURES then we descend, these are tests that ran but assertion failed.
    // we do not descend for aborted, these are tests that did not run because of previous failure.
    for (var child : tracker.children()) {
      var hasFailures = child.stats() != null && child.stats().failures() > 0;

      if (!child.runUri().leafType().descendantOf(TestUri.Segment.ENV)
          || hasFailures
          || parentFailures) {
        writeCompletedSummary(buffer, child, hasFailures);
      }
    }
  }

  /**
   * Writes the test node name, and it's throwable if it Failed, as in the assertions failed or it
   * threw an exception.
   *
   * <p>Example:
   *
   * <pre>
   * ✘ isDDLSuccess [body('$') - responseIsDDLSuccess: REQUIRED:[status], OPTIONAL:[], FORBIDDEN:[data, errors]] (node:aac)
   *
   * [responseIsDDLSuccess: REQUIRED:[status], OPTIONAL:[], FORBIDDEN:[data, errors]]
   * Expecting actual:
   *   {"errors"=[{"errorCode"="VECTORIZE_CREDENTIAL_INVALID", "family"="REQUEST", "id"="95b651e3-815b-4be3-8cad-3074fcd86cd9", "message"="Invalid credential name for vectorize, with error: Embedding Gateway unable to resolve authentication type.
   * Underlying problem: Sync service has internal server error. Error Code: 500; response description: Internal Server Error.      .", "scope"="SCHEMA", "title"="Invalid credential name for vectorize"}]}
   * to contain key:
   *   "status"
   * -----
   *
   * ✘ isDDLSuccess [body('$') - responseIsDDLSuccess: REQUIRED:[status], OPTIONAL:[], FORBIDDEN:[data, errors]] (node:aaQ)
   *
   * [responseIsDDLSuccess: REQUIRED:[status], OPTIONAL:[], FORBIDDEN:[data, errors]]
   * Expecting actual:
   *   {"errors"=[{"errorCode"="VECTORIZE_CREDENTIAL_INVALID", "family"="REQUEST", "id"="9d512a57-ca08-491b-8440-a5abc14b99a4", "message"="Invalid credential name for vectorize, with error: Embedding Gateway unable to resolve authentication type.
   * Underlying problem: Sync service has internal server error. Error Code: 500; response description: Internal Server Error.      .", "scope"="SCHEMA", "title"="Invalid credential name for vectorize"}]}
   * to contain key:
   *   "status"
   * -----
   * </pre>
   */
  private void writeFailureMessages(
      MessageBuilder buffer, DynamicTreeListener.TestReportingTracker tracker) {

    // if we have a throwable, write out the tree node test and the error it generated.
    // ignoring the ABORTED
    if (tracker.throwable().isPresent()
        && (tracker.junitStatus() == TestExecutionResult.Status.FAILED)) {
      writeTestDesc(buffer, tracker);
      buffer.newline();
      buffer.newline();
      buffer.a(tracker.throwable().get().getMessage());
      buffer.newline().a("-----").newline().newline();
    }
    tracker.children().forEach(child -> writeFailureMessages(buffer, child));
  }

  private void writeTestDesc(
      MessageBuilder buffer, DynamicTreeListener.TestReportingTracker tracker) {

    // Icon for success for failure,
    // if we have stats then this is a container we use that status, otherwise we use the JUNIT
    // execution status
    if (tracker.stats() == null) {
      switch (tracker.junitStatus()) {
        case SUCCESSFUL -> buffer.a(theme.successful());
        case FAILED, ABORTED -> buffer.a(theme.failed());
        case null -> {}
      }
    } else {
      if (tracker.stats().failures() == 0 && tracker.stats().aborted() == 0) {
        buffer.a(theme.successful());
      } else {
        buffer.a(theme.failed());
      }
    }

    // The name of the container or test
    buffer.strong(tracker.identifier().getDisplayName());

    // if we have stats write a stats line, these are aggregate for all things below.
    if (tracker.stats() != null) {
      writeTestStats(buffer, tracker);
    }

    // NOTE: does not add new line, caller should
  }

  private void writeTestStats(
      MessageBuilder buffer, DynamicTreeListener.TestReportingTracker tracker) {
    buffer
        .a(" - %s s".formatted(tracker.stats().elapsedMillis() / 1000))
        .a(theme.blank())
        .a("Successful: ")
        .a(tracker.stats().successful())
        .a(", ")
        .a("Failures: ")
        .a(tracker.stats().failures())
        .a(", ")
        .a("Aborted: ")
        .a(tracker.stats().aborted());
  }

  private void writeTreeOutline(
      MessageBuilder buffer, DynamicTreeListener.TestReportingTracker tracker) {

    // Indenting and tree building
    if (tracker.parent() == null) {
      buffer.a(theme.down());
    } else {
      buffer.a(theme.blank().repeat(tracker.depth() - 1)).a(theme.entry());
    }
  }
}
