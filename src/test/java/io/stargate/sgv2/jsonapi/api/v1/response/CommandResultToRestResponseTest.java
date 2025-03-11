package io.stargate.sgv2.jsonapi.api.v1.response;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.jboss.resteasy.reactive.RestResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CommandResultToRestResponseTest {

  @Nested
  class RestResponseMapper {

    @Test
    public void happyPath() {
      CommandResult result =
          CommandResult.statusOnlyBuilder(false, false, null)
              .addStatus(CommandStatus.OK, 1)
              .build();
      final RestResponse mappedResult = result.toRestResponse();
      assertThat(mappedResult.getStatus()).isEqualTo(RestResponse.Status.OK.getStatusCode());
    }

    @Test
    public void errorWithOkStatus() {
      CommandResult result =
          CommandResult.statusOnlyBuilder(false, false, null)
              .addCommandResultError(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.OK))
              .build();

      final RestResponse mappedResult = result.toRestResponse();
      assertThat(mappedResult.getStatus()).isEqualTo(RestResponse.Status.OK.getStatusCode());
    }

    @Test
    public void unauthorized() {
      CommandResult result =
          CommandResult.statusOnlyBuilder(false, false, null)
              .addCommandResultError(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.UNAUTHORIZED))
              .build();

      final RestResponse mappedResult = result.toRestResponse();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.UNAUTHORIZED.getStatusCode());
    }

    @Test
    public void badGateway() {
      CommandResult result =
          CommandResult.statusOnlyBuilder(false, false, null)
              .addCommandResultError(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.BAD_GATEWAY))
              .build();

      final RestResponse mappedResult = result.toRestResponse();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.BAD_GATEWAY.getStatusCode());
    }

    @Test
    public void internalError() {
      CommandResult result =
          CommandResult.statusOnlyBuilder(false, false, null)
              .addCommandResultError(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.INTERNAL_SERVER_ERROR))
              .build();

      final RestResponse mappedResult = result.toRestResponse();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    @Test
    public void gatewayError() {
      CommandResult result =
          CommandResult.statusOnlyBuilder(false, false, null)
              .addCommandResultError(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.GATEWAY_TIMEOUT))
              .build();

      final RestResponse mappedResult = result.toRestResponse();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.GATEWAY_TIMEOUT.getStatusCode());
    }
  }
}
