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
class DropKeyspaceCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void noName() throws Exception {
      String json =
          """
          {
            "dropKeyspace": {
            }
          }
          """;

      DropKeyspaceCommand command = objectMapper.readValue(json, DropKeyspaceCommand.class);
      Set<ConstraintViolation<DropKeyspaceCommand>> result = validator.validate(command);

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
            "dropKeyspace": {
              "name": "%s"
            }
          }
          """
              .formatted(RandomStringUtils.randomAlphabetic(49));

      DropKeyspaceCommand command = objectMapper.readValue(json, DropKeyspaceCommand.class);
      Set<ConstraintViolation<DropKeyspaceCommand>> result = validator.validate(command);

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
            "dropKeyspace": {
              "name": "_not_possible"
            }
          }
          """;

      DropKeyspaceCommand command = objectMapper.readValue(json, DropKeyspaceCommand.class);
      Set<ConstraintViolation<DropKeyspaceCommand>> result = validator.validate(command);

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
            "dropKeyspace": {
              "name": "my_space"
            }
          }
          """;

      DropKeyspaceCommand command = objectMapper.readValue(json, DropKeyspaceCommand.class);
      Set<ConstraintViolation<DropKeyspaceCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class DeprecatedDropKeyspaceCommandTest {

    @Test
    public void noName() throws Exception {
      String json =
          """
              {
                "dropNamespace": {
                }
              }
              """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

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
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(RandomStringUtils.randomAlphabetic(49));

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

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
                "dropNamespace": {
                  "name": "_not_possible"
                }
              }
              """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

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
                "dropNamespace": {
                  "name": "my_space"
                }
              }
              """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }
  }
}
