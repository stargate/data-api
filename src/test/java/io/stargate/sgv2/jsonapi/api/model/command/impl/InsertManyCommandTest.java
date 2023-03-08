package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Set;
import javax.inject.Inject;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class InsertManyCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void noDocuments() throws Exception {
      String json =
          """
          {
            "insertMany": {
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Set<ConstraintViolation<InsertManyCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }

    @Test
    public void documentsArrayEmpty() throws Exception {
      String json =
          """
          {
            "insertMany": {
              "documents": []
            }
          }
          """;

      InsertManyCommand command = objectMapper.readValue(json, InsertManyCommand.class);
      Set<ConstraintViolation<InsertManyCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be empty");
    }
  }
}
