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
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class CreateCollectionCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

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
