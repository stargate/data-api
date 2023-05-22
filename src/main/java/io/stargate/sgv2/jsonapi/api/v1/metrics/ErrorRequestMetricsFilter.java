package io.stargate.sgv2.jsonapi.api.v1.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.regex.Pattern;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;

/**
 * The filter for counting HTTP requests per tenant. Controlled by {@link
 * MetricsConfig.TenantRequestCounterConfig}.
 */
@ApplicationScoped
public class ErrorRequestMetricsFilter {

  // split pattern for the user agent, extract only first part of the agent
  private static final Pattern USER_AGENT_SPLIT = Pattern.compile("[\\s/]");

  // same as V1 io.stargate.core.metrics.StargateMetricConstants#UNKNOWN
  private static final String UNKNOWN_VALUE = "unknown";

  /** The {@link MeterRegistry} to report to. */
  private final MeterRegistry meterRegistry;

  /** The {@link ObjectMapper} to get command name from request. */
  private final ObjectMapper objectMapper;

  /** The tag for error being true, created only once. */
  private final Tag errorTrue = Tag.of("error", "true");

  /** Tag that represent the api module. */
  private final Tag apiTag = Tag.of("module", "jsonapi");

  /** The tag for error being false, created only once. */
  private final Tag errorFalse = Tag.of("error", "false");

  /** Default constructor. */
  @Inject
  public ErrorRequestMetricsFilter(MeterRegistry meterRegistry, ObjectMapper objectMapper) {
    this.meterRegistry = meterRegistry;
    this.objectMapper = objectMapper;
  }

  /**
   * Filter that this bean produces.
   *
   * @param requestContext {@link ContainerRequestContext}
   * @param responseContext {@link ContainerResponseContext}
   */
  @ServerResponseFilter
  public void record(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    String commandName = getCommandName(requestContext.getEntityStream());
    // resolve error
    boolean error = responseContext.getStatus() != 200 || checkForErrors(responseContext);
    Tag errorTag = error ? errorTrue : errorFalse;
    Tag commandTag = Tag.of("command", commandName);
    Tag statusCodeTag = Tag.of("statusCode", String.valueOf(responseContext.getStatus()));

    Tags tags = Tags.of(apiTag, commandTag, statusCodeTag, errorTag);
    // record
    meterRegistry.counter("http_server_requests_seconds_custom_count", tags).increment();
  }

  private String getCommandName(InputStream inputStream) {
    try {
      // reset the stream to fetch from beginning of stream
      inputStream.reset();
      // get body from requestContext
      final Iterator<String> fieldIterator = objectMapper.readTree(inputStream).fieldNames();
      return fieldIterator.hasNext() ? fieldIterator.next() : UNKNOWN_VALUE;
    } catch (IOException e) {
      return UNKNOWN_VALUE;
    }
  }

  /**
   * Checks if the response contains an error.
   *
   * @param responseContext {@link ContainerResponseContext}
   * @return true if the response contains an error, false otherwise
   */
  public boolean checkForErrors(ContainerResponseContext responseContext) {
    Object entity = responseContext.getEntity();
    if (entity instanceof CommandResult commandResult) {
      return commandResult.errors() != null && !commandResult.errors().isEmpty();
    }
    return false;
  }
}
