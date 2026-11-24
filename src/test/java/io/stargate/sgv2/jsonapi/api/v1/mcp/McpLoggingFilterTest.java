package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import org.jboss.logmanager.ExtLogRecord;
import org.junit.jupiter.api.Test;

class McpLoggingFilterTest {
  private static final Instant NOW = Instant.parse("2026-10-19T12:00:00Z");
  private static final String REQUEST_ID = "3480cda5-7ade-4f56-bc9f-d7d92407fb41";

  @Test
  void removesPayloadsFromMessagesParametersExceptionsAndContext() {
    var filter = new McpLoggingFilter(Optional.empty());
    var record = sensitiveRecord("io.stargate.sgv2.jsonapi.service.processor.CommandProcessor");

    assertThat(filter.isLoggable(record)).isTrue();
    assertThat(record.getFormattedMessage())
        .contains("exception=java.lang.IllegalArgumentException")
        .doesNotContain("synthetic-");
    assertThat(record.getParameters()).isNull();
    assertThat(record.getThrown()).isNull();
    assertThat(record.getMdcCopy())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                McpRequestLogging.REQUEST_ID,
                REQUEST_ID,
                McpLoggingFilter.EXCEPTION_TYPE,
                IllegalArgumentException.class.getName()));
    assertThat(record.getNdc()).isEmpty();
    assertThat(record.getMarker()).isNull();
    assertThat(record.getLevel()).isEqualTo(Level.WARNING);
  }

  @Test
  void protectsFrameworkFailuresWithoutARequestContext() {
    var record =
        sensitiveRecord("io.quarkiverse.mcp.server.http.runtime.StreamableHttpMcpMessageHandler");
    record.setMdc(Map.of());
    assertThat(new McpLoggingFilter(Optional.empty()).isLoggable(record)).isTrue();
    assertThat(record.getFormattedMessage()).doesNotContain("synthetic-");
    assertThat(record.getThrown()).isNull();
  }

  @Test
  void dropsRawTrafficEvenWhenDiagnosticsAreEnabled() {
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(NOW);
    var filter = new McpLoggingFilter(Optional.of(NOW.plusSeconds(60).toString()), clock);
    assertThat(filter.isLoggable(sensitiveRecord("io.quarkus.mcp.server.traffic"))).isFalse();
  }

  @Test
  void preservesExceptionTypeAcrossMultipleHandlers() {
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(NOW);
    for (var deadline :
        java.util.List.of(Optional.<String>empty(), Optional.of(NOW.plusSeconds(60).toString()))) {
      var record = sensitiveRecord("io.stargate.command");
      var firstHandler = new McpLoggingFilter(deadline, clock);
      var secondHandler = new McpLoggingFilter(deadline, clock);

      assertThat(firstHandler.isLoggable(record)).isTrue();
      assertThat(secondHandler.isLoggable(record)).isTrue();
      assertThat(record.getFormattedMessage())
          .contains("exception=java.lang.IllegalArgumentException")
          .doesNotContain("synthetic-");
    }
  }

  @Test
  void diagnosticsKeepOnlyBoundedStackLocationsAndExpire() {
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(NOW);
    var filter = new McpLoggingFilter(Optional.of(NOW.plusSeconds(60).toString()), clock);
    var record = sensitiveRecord("io.stargate.command");
    StackTraceElement frame = new StackTraceElement("example.Command", "run", "Command.java", 42);
    record
        .getThrown()
        .setStackTrace(java.util.Collections.nCopies(40, frame).toArray(StackTraceElement[]::new));

    assertThat(filter.isLoggable(record)).isTrue();
    assertThat(record.getThrown().getMessage()).doesNotContain("synthetic-");
    assertThat(record.getThrown().getCause()).isNull();
    assertThat(record.getThrown().getSuppressed()).isEmpty();
    assertThat(record.getThrown().getStackTrace()).hasSize(32).containsOnly(frame);

    when(clock.instant()).thenReturn(NOW.plusSeconds(60));
    var expired = sensitiveRecord("io.stargate.command");
    assertThat(filter.isLoggable(expired)).isTrue();
    assertThat(expired.getThrown()).isNull();
  }

  @Test
  void rejectsUnboundedOrMalformedDiagnosticConfigurationWithoutEchoingIt() {
    Clock clock = mock(Clock.class);
    when(clock.instant()).thenReturn(NOW);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new McpLoggingFilter(Optional.of(NOW.plusSeconds(901).toString()), clock))
        .withMessageContaining("at most 15 minutes");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new McpLoggingFilter(Optional.of("synthetic-config-secret"), clock))
        .withMessage("MCP diagnostics-until must be an ISO-8601 timestamp")
        .withNoCause();
  }

  @Test
  void leavesOrdinaryHttpAndBillingRecordsUnchanged() {
    var filter = new McpLoggingFilter(Optional.empty());
    var http = sensitiveRecord("io.stargate.http");
    http.setMdc(Map.of());
    var billing = sensitiveRecord("billing.events");
    for (var record : java.util.List.of(http, billing)) {
      String message = record.getMessage();
      Throwable failure = record.getThrown();
      assertThat(filter.isLoggable(record)).isTrue();
      assertThat(record.getMessage()).isEqualTo(message);
      assertThat(record.getThrown()).isSameAs(failure);
    }
  }

  private ExtLogRecord sensitiveRecord(String logger) {
    var record =
        new ExtLogRecord(Level.WARNING, "synthetic-message-secret %s", getClass().getName());
    record.setLoggerName(logger);
    record.setParameters(new Object[] {Map.of("access_token", "synthetic-parameter-secret")});
    var failure =
        new IllegalArgumentException(
            "synthetic-exception-secret", new RuntimeException("synthetic-cause-secret"));
    failure.addSuppressed(new IllegalStateException("synthetic-suppressed-secret"));
    record.setThrown(failure);
    record.setMdc(
        Map.of(McpRequestLogging.REQUEST_ID, REQUEST_ID, "tenantId", "synthetic-tenant-secret"));
    record.setNdc("synthetic-ndc-secret");
    record.setMarker("synthetic-marker-secret");
    return record;
  }
}
