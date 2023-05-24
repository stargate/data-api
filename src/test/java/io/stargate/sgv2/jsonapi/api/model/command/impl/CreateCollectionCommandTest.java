package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateCollectionCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void nameNull() throws Exception {
      String json =
          """
          {
            "createCollection": {
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Set<ConstraintViolation<CreateCollectionCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }

    @Test
    public void nameBlank() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name": ""
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Set<ConstraintViolation<CreateCollectionCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("size must be between 1 and 48");
    }

    @Test
    public void nameTooLong() throws Exception {
      String name = RandomStringUtils.randomAlphabetic(49);
      String json =
          """
          {
            "createCollection": {
              "name": "%s"
            }
          }
          """
              .formatted(name);

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Set<ConstraintViolation<CreateCollectionCommand>> result = validator.validate(command);

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
            "createCollection": {
              "name": "_not_possible"
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Set<ConstraintViolation<CreateCollectionCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .hasSize(1)
          .contains("must match \"[a-zA-Z][a-zA-Z0-9_]*\"");
    }

    @Test
    public void nameCorrectPattern() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name": "is_possible_10"
            }
          }
          """;

      CreateCollectionCommand command = objectMapper.readValue(json, CreateCollectionCommand.class);
      Set<ConstraintViolation<CreateCollectionCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }
  }
}
