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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateNamespaceCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void noName() throws Exception {
      String json =
          """
          {
            "createNamespace": {
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
    public void nameTooLong() throws Exception {
      String json =
              """
          {
            "createNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(RandomStringUtils.randomAlphabetic(49));

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Set<ConstraintViolation<CreateNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("size must be between 1 and 48");
    }

    @Test
    public void nameWrongPattern() throws Exception {
      String json =
          """
          {
            "createNamespace": {
              "name": "_not_possible"
            }
          }
          """;

      CreateNamespaceCommand command = objectMapper.readValue(json, CreateNamespaceCommand.class);
      Set<ConstraintViolation<CreateNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must match \"[a-zA-Z][a-zA-Z0-9_]*\"");
    }

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
