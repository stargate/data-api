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
class UpdateManyCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void noUpdateClause() throws Exception {
      String json =
          """
          {
            "updateMany": {
              "filter": {"name": "Aaron"}
            }
          }
          """;

      UpdateManyCommand command = objectMapper.readValue(json, UpdateManyCommand.class);
      Set<ConstraintViolation<UpdateManyCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }
  }
}
