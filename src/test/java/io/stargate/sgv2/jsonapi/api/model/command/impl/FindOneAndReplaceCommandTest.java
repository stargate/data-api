package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import java.util.Set;
import javax.inject.Inject;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneAndReplaceCommandTest {
  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
                  {
                    "findOneAndReplace": {
                        "filter" : {"username" : "update_user5"},
                        "replacement" : { "new_col": {"sub_doc_col" : "new_val2"}},
                        "options" : {}
                      }
                  }
                """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneAndReplaceCommand.class,
              findOneAndReplaceCommand -> {
                FilterClause filterClause = findOneAndReplaceCommand.filterClause();
                assertThat(filterClause).isNotNull();
                final JsonNode replacementDocument = findOneAndReplaceCommand.replacementDocument();
                assertThat(replacementDocument).isNotNull();
                final FindOneAndReplaceCommand.Options options = findOneAndReplaceCommand.options();
                assertThat(options).isNotNull();
              });
    }

    @Test
    public void withSortAndOptions() throws Exception {
      String json =
          """
                  {
                    "findOneAndReplace": {
                        "filter" : {"username" : "update_user5"},
                        "sort" : ["location"],
                        "replacement" : { "new_col": {"sub_doc_col" : "new_val2"}},
                        "options" : {"returnDocument" : "after"}
                      }
                  }
                  """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneAndReplaceCommand.class,
              findOneAndReplaceCommand -> {
                FilterClause filterClause = findOneAndReplaceCommand.filterClause();
                assertThat(filterClause).isNotNull();
                final JsonNode replacementDocument = findOneAndReplaceCommand.replacementDocument();
                assertThat(replacementDocument).isNotNull();
                final FindOneAndReplaceCommand.Options options = findOneAndReplaceCommand.options();
                assertThat(options).isNotNull();
                assertThat(options.returnDocument()).isNotNull();
                assertThat(options.returnDocument()).isEqualTo("after");
              });
    }

    @Test
    public void invalidReturnDocumentOption() throws Exception {
      String json =
          """
                {
                  "findOneAndReplace": {
                    "filter": {"name": "Aaron"},
                    "replacement": { "col" : "val"},
                    "options": {
                        "returnDocument": "yes"
                    }
                  }
                }
                """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Set<ConstraintViolation<FindOneAndReplaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("returnDocument value can only be 'before' or 'after'");
    }
  }
}
