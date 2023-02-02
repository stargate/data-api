package io.stargate.sgv2.jsonapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
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
              "filter": {"username": "aaron"},
              "options": {}
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
              },
              "options" :{}
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

  @Nested
  class CreateCollection {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
                {
                  "createCollection": {
                    "name": "some_name",
                    "options" :{}
                  }
                }
                """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionCommand.class,
              createCollection -> {
                String name = createCollection.name();
                assertThat(name).isNotNull();
              });
    }
  }

  @Nested
  class Find {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
                    {
                      "find": {
                        "filter" : {"username" : "user1"},
                        "options" : {
                          "pageSize" : 1
                        }
                      }
                    }
                    """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindCommand.class,
              find -> {
                FilterClause filterClause = find.filterClause();
                assertThat(filterClause).isNotNull();
                final FindCommand.Options options = find.options();
                assertThat(options).isNotNull();
                assertThat(options.pageSize()).isEqualTo(1);
              });
    }
  }

  @Nested
  class UpdateOne {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
                    {
                      "updateOne": {
                          "filter" : {"username" : "update_user5"},
                          "update" : {"$set" : {"new_col": {"sub_doc_col" : "new_val2"}}},
                          "options" : {}
                        }
                    }
                    """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              UpdateOneCommand.class,
              updateOneCommand -> {
                FilterClause filterClause = updateOneCommand.filterClause();
                assertThat(filterClause).isNotNull();
                final UpdateClause updateClause = updateOneCommand.updateClause();
                assertThat(updateClause).isNotNull();
                assertThat(updateClause.buildOperations()).hasSize(1);
                final UpdateOneCommand.Options options = updateOneCommand.options();
                assertThat(options).isNotNull();
              });
    }
  }

  @Nested
  class FindOneAndUpdate {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
                    {
                      "findOneAndUpdate": {
                          "filter" : {"username" : "update_user5"},
                          "update" : {"$set" : {"new_col": {"sub_doc_col" : "new_val2"}}},
                          "options" : {}
                        }
                    }
                    """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneAndUpdateCommand.class,
              findOneAndUpdateCommand -> {
                FilterClause filterClause = findOneAndUpdateCommand.filterClause();
                assertThat(filterClause).isNotNull();
                final UpdateClause updateClause = findOneAndUpdateCommand.updateClause();
                assertThat(updateClause).isNotNull();
                assertThat(updateClause.buildOperations()).hasSize(1);
                final FindOneAndUpdateCommand.Options options = findOneAndUpdateCommand.options();
                assertThat(options).isNotNull();
              });
    }
  }

  @Nested
  class InsertMany {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
                    {
                      "insertMany": {
                          "documents": [{
                            "_id" : "1",
                            "some": {
                              "data": true
                            }
                          },
                          {
                            "_id" : "2",
                            "some": {
                              "data": false
                            }
                          }],
                          "options" :{}
                        }
                    }
                    """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              InsertManyCommand.class,
              insertManyCommand -> {
                final List<JsonNode> documents = insertManyCommand.documents();
                assertThat(documents).isNotNull();
                assertThat(documents).hasSize(2);
                final InsertManyCommand.Options options = insertManyCommand.options();
                assertThat(options).isNotNull();
              });
    }
  }
}
