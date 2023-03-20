package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

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
class FindOneAndUpdateCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void noUpdateClause() throws Exception {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter": {"name": "Aaron"}
            }
          }
          """;

      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      Set<ConstraintViolation<FindOneAndUpdateCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }

    @Test
    public void invalidReturnDocumentOption() throws Exception {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter": {"name": "Aaron"},
              "update": { "$set": {"name": "Tatu"}},
              "options": {
                  "returnDocument": "yes"
              }
            }
          }
          """;

      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      Set<ConstraintViolation<FindOneAndUpdateCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("returnDocument value can only be 'before' or 'after'");
    }

    @Test
    public void validReturnDocumentOption() throws Exception {
      String json =
          """
          {
            "findOneAndUpdate": {
              "filter": {"name": "Aaron"},
              "update": { "$set": {"name": "Tatu"}},
              "options": {
                  "returnDocument": "after"
              }
            }
          }
          """;

      FindOneAndUpdateCommand command = objectMapper.readValue(json, FindOneAndUpdateCommand.class);
      Set<ConstraintViolation<FindOneAndUpdateCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }
  }
}
