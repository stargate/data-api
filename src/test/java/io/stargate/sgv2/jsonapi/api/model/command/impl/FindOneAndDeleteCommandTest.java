package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneAndDeleteCommandTest {
  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  private final TestConstants testConstants = new TestConstants();

  @Nested
  class Validation {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "findOneAndDelete": {
                "filter" : {"username" : "update_user5"}
              }
          }
          """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneAndDeleteCommand.class,
              findOneAndDeleteCommand -> {
                assertThat(findOneAndDeleteCommand.filterDefinition()).isNotNull();
              });
    }

    @Test
    public void withSort() throws Exception {
      String json =
          """
          {
            "findOneAndDelete": {
                "filter" : {"username" : "update_user5"},
                "sort" : {"location" : 1}
              }
          }
          """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneAndDeleteCommand.class,
              findOneAndDeleteCommand -> {
                assertThat(findOneAndDeleteCommand.filterDefinition()).isNotNull();
                final SortClause sortClause =
                    findOneAndDeleteCommand.sortClause(testConstants.collectionContext());
                assertThat(sortClause).isNotNull();
                assertThat(sortClause)
                    .satisfies(
                        sort -> {
                          assertThat(sort.sortExpressions()).hasSize(1);
                          assertThat(sort.sortExpressions().get(0).getPath()).isEqualTo("location");
                        });
              });
    }

    @Test
    public void sortAndProject() throws Exception {
      String json =
          """
          {
            "findOneAndDelete": {
                "filter" : {"username" : "update_user5"},
                "sort" : {"location" : 1},
                "projection" : {"username" : 1}
            }
          }
          """;

      Command result = objectMapper.readValue(json, Command.class);
      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneAndDeleteCommand.class,
              findOneAndDeleteCommand -> {
                assertThat(findOneAndDeleteCommand.filterDefinition()).isNotNull();
                final SortClause sortClause =
                    findOneAndDeleteCommand.sortClause(testConstants.collectionContext());
                assertThat(sortClause).isNotNull();
                assertThat(sortClause)
                    .satisfies(
                        sort -> {
                          assertThat(sort.sortExpressions()).hasSize(1);
                          assertThat(sort.sortExpressions().get(0).getPath()).isEqualTo("location");
                        });
                final JsonNode projector = findOneAndDeleteCommand.projectionDefinition();
                assertThat(projector).isNotNull();
                assertThat(projector)
                    .satisfies(
                        project -> {
                          assertThat(project)
                              .isEqualTo(objectMapper.readTree("{\"username\" : 1}"));
                        });
              });
    }
  }
}
