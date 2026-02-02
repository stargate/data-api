package io.stargate.sgv2.jsonapi.api.v1.response;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandError;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.ws.rs.core.Response;
import java.util.UUID;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CommandResultToRestResponseTest {

  @Nested
  class RestResponseMapper {

    @Test
    public void happyPath() {
      var commandResult =
          CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
              .addStatus(CommandStatus.OK, 1)
              .build();

      assertThat(commandResult.toRestResponse().getStatus())
          .as("Default HTTP status is 200 OK")
          .isEqualTo(RestResponse.Status.OK.getStatusCode());
    }

    @Test
    public void okStatusWithThrowable() {

      var commandResult =
          CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
              .addThrowable(new RuntimeException("test exception"))
              .build();

      assertThat(commandResult.toRestResponse().getStatus())
          .as("Default HTTP status is 200 OK with throwable added")
          .isEqualTo(RestResponse.Status.OK.getStatusCode());
    }

    @Test
    public void non200StatusPassedThrough() {

      var commandResult =
          CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
              .addCommandError(
                  CommandError.builder()
                      .id(UUID.randomUUID())
                      .family("TEST_FAMILY")
                      .scope("TEST_SCOPE")
                      .errorCode("TEST_CODE")
                      .title("Test Title")
                      .message("Test Message")
                      .httpStatus(Response.Status.UNAUTHORIZED)
                      .build())
              .build();

      assertThat(commandResult.toRestResponse().getStatus())
          .as("Non-200 HTTP status set on CommandError is set as RestResponse status")
          .isEqualTo(RestResponse.Status.UNAUTHORIZED.getStatusCode());
    }
  }
}
