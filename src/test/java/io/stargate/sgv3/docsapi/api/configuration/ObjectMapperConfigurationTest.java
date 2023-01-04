package io.stargate.sgv3.docsapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.InsertOneCommand;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class ObjectMapperConfigurationTest {

  @Inject ObjectMapper objectMapper;

  @Nested
  class FindOne {

    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "findOne": {
              "sort": [
                "user.name",
                "-user.age"
              ],
              "filter": {"username": "aaron"}
            }
          }
          """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneCommand.class,
              findOne -> {
                SortClause sortClause = findOne.sortClause();
                assertThat(sortClause).isNotNull();
                assertThat(sortClause.sortExpressions())
                    .contains(
                        new SortExpression("user.name", true),
                        new SortExpression("user.age", false));
              });
      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneCommand.class,
              findOne -> {
                FilterClause filterClause = findOne.filter();
                assertThat(filterClause).isNotNull();
                assertThat(filterClause.comparisonExpressions()).hasSize(1);
                assertThat(filterClause.comparisonExpressions().get(0).path())
                    .isEqualTo("username");
                assertThat(
                        ((ValueComparisonOperation)
                                filterClause
                                    .comparisonExpressions()
                                    .get(0)
                                    .filterOperations()
                                    .get(0))
                            .operator())
                    .isEqualTo(ValueComparisonOperator.EQ);
                assertThat(
                        ((ValueComparisonOperation)
                                filterClause
                                    .comparisonExpressions()
                                    .get(0)
                                    .filterOperations()
                                    .get(0))
                            .rhsOperand()
                            .getTypedValue()
                            .toString())
                    .isEqualTo("aaron");
              });
    }

    @Test
    public void sortClauseOptional() throws Exception {
      String json =
          """
          {
            "findOne": {
            }
          }
          """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneCommand.class, findOne -> assertThat(findOne.sortClause()).isNull());
    }

    @Test
    public void filterClauseOptional() throws Exception {
      String json =
          """
                  {
                    "findOne": {
                    }
                  }
                  """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneCommand.class, findOne -> assertThat(findOne.filter()).isNull());
    }
  }

  @Nested
  class InsertOne {

    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "some": {
                  "data": true
                }
              }
            }
          }
          """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertOneCommand.class,
              insertOne -> {
                JsonNode document = insertOne.document();
                assertThat(document).isNotNull();
                assertThat(document.required("some").required("data").asBoolean()).isTrue();
              });
    }
  }
}
