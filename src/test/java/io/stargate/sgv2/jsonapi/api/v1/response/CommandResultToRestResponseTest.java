package io.stargate.sgv2.jsonapi.api.v1.response;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import jakarta.ws.rs.core.Response;
import java.util.List;
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
      CommandResult result = new CommandResult(Map.of(CommandStatus.OK, 1));
      final RestResponse mappedResult = result.map();
      assertThat(mappedResult.getStatus()).isEqualTo(RestResponse.Status.OK.getStatusCode());
    }

    @Test
    public void errorWithOkStatus() {
      CommandResult result =
          new CommandResult(
              List.of(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.OK)));
      final RestResponse mappedResult = result.map();
      assertThat(mappedResult.getStatus()).isEqualTo(RestResponse.Status.OK.getStatusCode());
    }

    @Test
    public void unauthorized() {
      CommandResult result =
          new CommandResult(
              List.of(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.UNAUTHORIZED)));
      final RestResponse mappedResult = result.map();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.UNAUTHORIZED.getStatusCode());
    }

    @Test
    public void badGateway() {
      CommandResult result =
          new CommandResult(
              List.of(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.BAD_GATEWAY)));
      final RestResponse mappedResult = result.map();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.BAD_GATEWAY.getStatusCode());
    }

    @Test
    public void internalError() {
      CommandResult result =
          new CommandResult(
              List.of(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.INTERNAL_SERVER_ERROR)));
      final RestResponse mappedResult = result.map();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    @Test
    public void gatewayError() {
      CommandResult result =
          new CommandResult(
              List.of(
                  new CommandResult.Error(
                      "My message.",
                      Map.of("field", "value"),
                      Map.of("field", "value"),
                      Response.Status.GATEWAY_TIMEOUT)));
      final RestResponse mappedResult = result.map();
      assertThat(mappedResult.getStatus())
          .isEqualTo(RestResponse.Status.GATEWAY_TIMEOUT.getStatusCode());
    }
  }
}
