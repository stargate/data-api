package io.stargate.sgv2.jsonapi.api.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.security.challenge.ChallengeSender;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
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

  @Inject
  public ErrorChallengeSender(
      @ConfigProperty(name = "stargate.auth.header-based.header-name", defaultValue = "")
          String headerName,
      ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;

    // create the response
    String message =
        "Role unauthorized for operation: Missing token, expecting one in the %s header."
            .formatted(headerName);
    CommandResult.Error error = new CommandResult.Error(message);
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

      // always set status to 200
      context.response().setStatusCode(200);

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
