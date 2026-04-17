package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import static org.apache.maven.surefire.shared.utils.logging.MessageUtils.buffer;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import org.apache.maven.plugin.surefire.report.Theme;
import org.apache.maven.surefire.shared.utils.logging.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBenchConsoleWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestBenchConsoleWriter.class);

  private boolean firstLine = true;

  private final Theme theme;

  public TestBenchConsoleWriter() {
    this.theme = Theme.EMOJI;
  }

  /**
   * We print the test has starting , so the output from the test comes after, we then output the
   * summary later.
   *
   * @param tracker
   */
  public void printTestStarted(DynamicTreeListener.TestTracker tracker) {

    var buffer = buffer();
    if (firstLine) {

      firstLine = false;
      buffer
          .newline()
          .a(theme.dash().repeat(20))
          .newline()
          .a("Running Test Bench, summary shown at completion...")
          .newline()
          .a(theme.dash().repeat(20))
          .newline();
    }

    //        [INFO] ── io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommandTest -
    // 1.316 s
    //        [INFO]    ├─ ✔ noDocuments - 0.274 s

    // if there is no parent
    if (tracker.parent() == null) {
      buffer.a(theme.dash()).strong(tracker.identifier().getDisplayName());
    } else {
      buffer
          .a(theme.blank().repeat(tracker.depth() - 1))
          .a(theme.entry())
          .strong(tracker.identifier().getDisplayName());
    }
    LOGGER.info(buffer.toString());
  }

  public void printTestPlanCompleted(DynamicTreeListener.TestTracker rootTracker) {

    var buffer = buffer();

    buffer
        .newline()
        .a(theme.dash().repeat(20))
        .newline()
        .a("Test Bench Summary")
        .newline()
        .a(theme.dash().repeat(20))
        .newline();

    //        [INFO] ── io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommandTest -
    // 1.316 s
    //        [INFO]    ├─ ✔ noDocuments - 0.274 s

    // if there is no parent
    //        if (tracker.parent() == null){
    //            buffer.a(theme.dash())
    //                    .strong(tracker.identifier().getDisplayName());
    //        }
    //        else {
    //            buffer.a(theme.blank().repeat(tracker.depth() -1))
    //                    .a(theme.entry())
    //                    .strong(tracker.identifier().getDisplayName());
    //        }
    writeCompletedSummary(buffer, rootTracker, true);
    LOGGER.info(buffer.toString());
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
      MessageBuilder buffer, DynamicTreeListener.TestTracker tracker, boolean isRoot) {

    // the tree part
    if (isRoot) {
      buffer.a(theme.dash());
    } else {
      buffer.a(theme.blank().repeat(tracker.depth() - 1)).a(theme.entry());
    }

    var timing =
        tracker.stats() == null ? "" : " - %s s".formatted(tracker.stats().elapsedMillis() / 1000);
    // name of the group
    if (tracker.stats().noErrors()) {
      buffer.a(theme.successful());
    } else {
      buffer.a(theme.failed());
    }

    buffer.strong(tracker.identifier().getDisplayName()).a(timing).newline();

    // If we have a TestEnv then we want to write out the summary of results for it, otherwise
    // descend until we get one
    if (tracker.runUri().leafType() == TestUri.Segment.ENV) {

      buffer
          .a(theme.blank().repeat(tracker.depth()))
          .a(theme.details())
          .a("Successful: " + tracker.stats().successful())
          .newline();
      buffer
          .a(theme.blank().repeat(tracker.depth()))
          .a(theme.details())
          .a("Failures: " + tracker.stats().failures())
          .newline();
      buffer
          .a(theme.blank().repeat(tracker.depth()))
          .a(theme.details())
          .a("Aborted: " + tracker.stats().aborted())
          .newline();
      buffer
          .a(theme.blank().repeat(tracker.depth()))
          .a(theme.details())
          .a("Skipped: " + tracker.stats().skipped())
          .newline();
    } else {
      tracker.children().forEach(child -> writeCompletedSummary(buffer, child, false));
    }
  }
}
