package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.io.IOException;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class UpdateOneCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
        {
          "updateOne": {
              "filter" : {"username" : "update_user5"},
              "update" : {"$set" : {"new_col": {"sub_doc_col" : "new_val2"}}},
              "sort" : {"username" : 1},
              "options" : {}
            }
        }
        """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              UpdateOneCommand.class,
              updateOneCommand -> {
                assertThat(updateOneCommand.filterDefinition()).isNotNull();
                final UpdateClause updateClause = updateOneCommand.updateClause();
                assertThat(updateClause).isNotNull();
                assertThat(updateClause.buildOperations()).hasSize(1);
                assertThat(updateOneCommand.sortDefinition()).isNotNull();
                assertThat(updateOneCommand.sortDefinition().json())
                    .isEqualTo(readTree("{\"username\" : 1}"));
                assertThat(updateOneCommand.options()).isNotNull();
              });
    }

    @Test
    public void noUpdateClause() throws Exception {
      String json =
          """
          {
            "updateOne": {
              "filter": {"name": "Aaron"}
            }
          }
          """;

      UpdateOneCommand command = objectMapper.readValue(json, UpdateOneCommand.class);
      Set<ConstraintViolation<UpdateOneCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }
  }

  private JsonNode readTree(String json) {
    try {
      return objectMapper.readTree(json);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse JSON", e);
    }
  }
}
