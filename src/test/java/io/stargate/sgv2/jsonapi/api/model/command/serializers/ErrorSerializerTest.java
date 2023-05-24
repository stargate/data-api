package io.stargate.sgv2.jsonapi.api.model.command.serializers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class ErrorSerializerTest {

  @Inject ObjectMapper objectMapper;

  @Nested
  class Serialize {

    @Test
    public void happyPath() throws Exception {
      CommandResult.Error error =
          new CommandResult.Error("My message.", Map.of("field", "value"), Response.Status.OK);

      String result = objectMapper.writeValueAsString(error);

      assertThat(result)
          .isEqualTo("""
                    {"message":"My message.","field":"value"}""");
    }

    @Test
    public void withoutProps() throws Exception {
      CommandResult.Error error = new CommandResult.Error("My message.");

      String result = objectMapper.writeValueAsString(error);

      assertThat(result).isEqualTo("""
                    {"message":"My message."}""");
    }

    @Test
    public void messageFieldNotAllowed() throws Exception {
      Throwable throwable =
          catchThrowable(
              () ->
                  new CommandResult.Error(
                      "My message.", Map.of("message", "value"), Response.Status.OK));

      assertThat(throwable)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Error fields can not contain the reserved message key.");
    }

    @Test
    public void withNulls() throws Exception {
      CommandResult.Error error = new CommandResult.Error(null, null, Response.Status.OK);

      String result = objectMapper.writeValueAsString(error);

      assertThat(result).isEqualTo("""
                    {"message":null}""");
    }
  }
}
