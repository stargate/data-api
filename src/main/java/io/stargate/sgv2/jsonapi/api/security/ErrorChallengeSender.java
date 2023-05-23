package io.stargate.sgv2.jsonapi.api.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.security.challenge.ChallengeSender;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConstants;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responds with {@link CommandResult} containing an error on send challenge. */
@ApplicationScoped
public class ErrorChallengeSender implements ChallengeSender {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorChallengeSender.class);

  /** Object mapper for custom response. */
  private final ObjectMapper objectMapper;

  /** Result is always constant */
  private final CommandResult commandResult;

  /** The {@link MeterRegistry} to report to. */
  private final MeterRegistry meterRegistry;

  /** The tag for error being true, created only once. */
  private final Tag errorTag = Tag.of("error", "true");

  /** Tag that represent the api module. */
  private final Tag apiTag = Tag.of("module", "jsonapi");

  /**
   * The command name tag for auth token missing. Don't have access to request message body, so
   * hardcoded to unknown
   */
  private final Tag commandTag = Tag.of("command", MetricsConstants.UNKNOWN_VALUE);

  /** The tag for status code, as only 401 will be returned from here */
  private final Tag statusCodeTag = Tag.of("statusCode", String.valueOf(401));

  @Inject
  public ErrorChallengeSender(
      @ConfigProperty(name = "stargate.auth.header-based.header-name", defaultValue = "")
          String headerName,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry) {
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
    // create the response
    String message =
        "Role unauthorized for operation: Missing token, expecting one in the %s header."
            .formatted(headerName);
    CommandResult.Error error =
        new CommandResult.Error(message, Collections.emptyMap(), Response.Status.UNAUTHORIZED);
    commandResult = new CommandResult(List.of(error));
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Boolean> apply(RoutingContext context, ChallengeData challengeData) {
    try {
      // try to serialize
      String response = objectMapper.writeValueAsString(commandResult);

      // set content type
      context.response().headers().set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);

      // set content length
      context
          .response()
          .headers()
          .set(HttpHeaders.CONTENT_LENGTH, String.valueOf(response.getBytes().length));

      // Return the status code from the challenge data
      context.response().setStatusCode(challengeData.status);

      // Add metrics
      Tags tags = Tags.of(apiTag, commandTag, statusCodeTag, errorTag);
      // record
      meterRegistry.counter("http_server_requests_custom_seconds_count", tags).increment();

      // write and map to true
      return Uni.createFrom()
          .completionStage(context.response().write(response).map(true).toCompletionStage());
    } catch (JsonProcessingException e) {
      LOG.error("Unable to serialize CommandResult instance {} to JSON.", commandResult, e);

      // keep original status in case we failed
      context.response().setStatusCode(challengeData.status);
      return Uni.createFrom().item(true);
    }
  }
}
