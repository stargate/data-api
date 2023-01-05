package io.stargate.sgv3.docsapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.InsertOneCommand;
import java.util.List;
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
      final ComparisonExpression expectedResult =
          new ComparisonExpression(
              "username",
              List.of(
                  new ValueComparisonOperation(
                      ValueComparisonOperator.EQ, new JsonLiteral("aaron", JsonType.STRING))));
      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneCommand.class,
              findOne -> {
                FilterClause filterClause = findOne.filterClause();
                assertThat(filterClause).isNotNull();
                assertThat(filterClause.comparisonExpressions()).hasSize(1);
                assertThat(filterClause.comparisonExpressions())
                    .singleElement()
                    .isEqualTo(expectedResult);
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
              FindOneCommand.class, findOne -> assertThat(findOne.filterClause()).isNull());
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
