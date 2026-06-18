package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateKeyspaceCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void nameCorrectPattern() throws Exception {
      String json =
          """
          {
            "createKeyspace": {
              "name": "is_possible_10"
            }
          }
          """;

      CreateKeyspaceCommand command = objectMapper.readValue(json, CreateKeyspaceCommand.class);
      Set<ConstraintViolation<CreateKeyspaceCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }

    @Test
    public void strategyNull() throws Exception {
      String json =
          """
          {
            "createKeyspace": {
              "name": "red_star_belgrade",
              "options": {
                "replication": {
                }
              }
            }
          }
          """;

      CreateKeyspaceCommand command = objectMapper.readValue(json, CreateKeyspaceCommand.class);
      Set<ConstraintViolation<CreateKeyspaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }

    @Test
    public void strategyWrong() throws Exception {
      String json =
          """
          {
            "createKeyspace": {
              "name": "red_star_belgrade",
              "options": {
                "replication": {
                    "class": "MyClass"
                }
              }
            }
          }
          """;

      CreateKeyspaceCommand command = objectMapper.readValue(json, CreateKeyspaceCommand.class);
      Set<ConstraintViolation<CreateKeyspaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must match \"SimpleStrategy|NetworkTopologyStrategy\"");
    }
  }

  @Nested
  class DeprecatedCreateNamespace {

    @Test
    public void nameCorrectPattern() throws Exception {
      String json =
          """
              {
                "createNamespace": {
                  "name": "is_possible_10"
                }
              }
              """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Set<ConstraintViolation<CreateNamespaceCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }

    @Test
    public void strategyNull() throws Exception {
      String json =
          """
              {
                "createNamespace": {
                  "name": "red_star_belgrade",
                  "options": {
                    "replication": {
                    }
                  }
                }
              }
              """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Set<ConstraintViolation<CreateNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }

    @Test
    public void strategyWrong() throws Exception {
      String json =
          """
              {
                "createNamespace": {
                  "name": "red_star_belgrade",
                  "options": {
                    "replication": {
                        "class": "MyClass"
                    }
                  }
                }
              }
              """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Set<ConstraintViolation<CreateNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must match \"SimpleStrategy|NetworkTopologyStrategy\"");
    }
  }
}
