package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.logging.LoggingFilter;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.logging.Filter;
import java.util.logging.LogRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logmanager.ExtLogRecord;

/**
 * Keeps free-form messages, parameters, exception messages and context out of MCP logs. Applying
 * this at the handler also covers framework parsing errors and asynchronous command failures.
 */
@LoggingFilter(name = "mcp-safe-logging")
public final class McpLoggingFilter implements Filter {
  private static final String SAFE_LOGGER = McpRequestLogging.class.getName();
  static final String EXCEPTION_TYPE = "mcpExceptionType";
  private final Instant diagnosticsUntil;
  private final Clock clock;

  @Inject
  public McpLoggingFilter(
      @ConfigProperty(name = "stargate.jsonapi.mcp.diagnostics-until")
          Optional<String> diagnosticsUntil) {
    this(diagnosticsUntil, Clock.systemUTC());
  }

  McpLoggingFilter(Optional<String> diagnosticsUntil, Clock clock) {
    this.clock = clock;
    try {
      this.diagnosticsUntil = diagnosticsUntil.map(Instant::parse).orElse(Instant.MIN);
    } catch (DateTimeParseException e) {
      // Do not include an invalid configuration value in an error message.
      throw new IllegalArgumentException("MCP diagnostics-until must be an ISO-8601 timestamp");
    }
    if (this.diagnosticsUntil.isAfter(clock.instant().plus(Duration.ofMinutes(15)))) {
      throw new IllegalArgumentException("MCP diagnostics may be enabled for at most 15 minutes");
    }
  }

  @Override
  public boolean isLoggable(LogRecord record) {
    String logger = record.getLoggerName();
    // Raw traffic logging must stay off even if an operator enables the framework setting.
    if ("io.quarkus.mcp.server.traffic".equals(logger)) {
      return false;
    }
    var extended = record instanceof ExtLogRecord r ? r : null;
    String requestId = extended == null ? null : extended.getMdc(McpRequestLogging.REQUEST_ID);
    boolean framework = logger != null && logger.startsWith("io.quarkiverse.mcp.server.");
    if ((!framework && requestId == null) || "billing.events".equals(logger)) {
      return true;
    }

    Throwable failure = record.getThrown();
    // A record can pass through this filter on multiple handlers. Preserve the original type
    // after the first handler has removed or replaced the throwable.
    String exceptionType = extended == null ? null : extended.getMdc(EXCEPTION_TYPE);
    if (exceptionType == null && failure != null) {
      exceptionType = failure.getClass().getName();
    }

    // MDC/NDC may include user-controlled tenant, collection and other input values.
    if (extended != null) {
      var safeContext = new HashMap<String, String>();
      if (requestId != null) {
        safeContext.put(McpRequestLogging.REQUEST_ID, requestId);
      }
      if (exceptionType != null) {
        safeContext.put(EXCEPTION_TYPE, exceptionType);
      }
      extended.setMdc(safeContext);
      extended.setNdc("");
      extended.setMarker(null);
    }
    if (SAFE_LOGGER.equals(logger)) {
      return true;
    }

    record.setMessage(
        "MCP event details redacted"
            + (exceptionType == null ? "" : "; exception=" + exceptionType));
    record.setParameters(null);
    record.setResourceBundle(null);
    record.setResourceBundleName(null);
    record.setThrown(null);
    if (failure != null && clock.instant().isBefore(diagnosticsUntil)) {
      // Preserve stack locations without copying messages, causes or suppressed exceptions.
      Throwable safeFailure = new Throwable("MCP diagnostic stack (details redacted)");
      safeFailure.setStackTrace(
          Arrays.stream(failure.getStackTrace()).limit(32).toArray(StackTraceElement[]::new));
      record.setThrown(safeFailure);
    }
    return true;
  }
}
