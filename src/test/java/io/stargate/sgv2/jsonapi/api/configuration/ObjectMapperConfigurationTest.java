package io.stargate.sgv2.jsonapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateEmbeddingServiceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.inject.Inject;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class ObjectMapperConfigurationTest {

  @Inject ObjectMapper objectMapper;

  @Inject DocumentLimitsConfig documentLimitsConfig;

  @Nested
  class FindOne {

    @Test
    public void happyPath() throws Exception {
      String json =
          """
              {
                "findOne": {
                  "sort": {
                    "user.name" : 1,
                    "user.age" : -1
                  },
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
                        SortExpression.sort("user.name", true),
                        SortExpression.sort("user.age", false));

                FilterClause filterClause = findOne.filterClause();
                assertThat(filterClause).isNotNull();
                assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount())
                    .isEqualTo(1);
                assertThat(filterClause.logicalExpression().comparisonExpressions)
                    .singleElement()
                    .satisfies(
                        expression -> {
                          ValueComparisonOperation<String> op =
                              new ValueComparisonOperation<>(
                                  ValueComparisonOperator.EQ,
                                  new JsonLiteral<>("aaron", JsonType.STRING));

                          assertThat(expression.getPath()).isEqualTo("username");
                          assertThat(expression.getFilterOperations())
                              .singleElement()
                              .isEqualTo(op);
                        });
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
              FindOneCommand.class,
              findOne -> Assertions.assertThat(findOne.sortClause()).isNull());
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
              FindOneCommand.class,
              findOne -> Assertions.assertThat(findOne.filterClause()).isNull());
    }

    // Only "empty" Options allowed, nothing else
    @Test
    public void findOneWithIncludeSimilarity() throws Exception {
      String json =
          """
        {
          "findOne": {
              "options": {
                  "includeSimilarity": true
              }
          }
        }
        """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              FindOneCommand.class,
              findOne -> {
                assertThat(findOne.options()).isNotNull();
                assertThat(findOne.options().includeSimilarity()).isTrue();
              });
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

    // Only "empty" Options allowed, nothing else
    @Test
    public void failForNonEmptyOptions() throws Exception {
      String json =
          """
                  {
                    "insertOne": {
                      "document": {
                        "some": {
                          "data": true
                        }
                      },
                      "options": {
                        "noSuchOption": "value"
                      }
                    }
                  }
                """;

      Exception e = catchException(() -> objectMapper.readValue(json, Command.class));
      assertThat(e)
          .isInstanceOf(JsonMappingException.class)
          .hasMessageStartingWith(
              ErrorCode.COMMAND_ACCEPTS_NO_OPTIONS.getMessage() + ": InsertOneCommand");
    }

    @Test
    public void failForTooLongNumbers() {
      String tooLongNumStr = "1234567890".repeat(6);
      String json =
          """
                {
                  "insertOne": {
                    "document": {
                       "_id" : 123,
                       "bigNumber" : %s
                    }
                  }
                }
                """
              .formatted(tooLongNumStr);

      Exception e = catchException(() -> objectMapper.readValue(json, Command.class));
      // Without exception mappers we just get default Jackson JsonMappingException wrapping the
      // constraints violation exception
      assertThat(e)
          .isInstanceOf(JsonMappingException.class)
          .hasMessageContaining(
              "Number length ("
                  + tooLongNumStr.length()
                  + ") exceeds the maximum length ("
                  + documentLimitsConfig.maxNumberLength()
                  + ")");
    }
  }

  @Nested
  class DeleteOne {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
                  {
                    "deleteOne": {
                      "filter" : {"username" : "Aaron"}
                    }
                  }
                """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              DeleteOneCommand.class,
              cmd -> {
                FilterClause filterClause = cmd.filterClause();
                assertThat(filterClause).isNotNull();
                assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount())
                    .isEqualTo(1);

                assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
                    .isEqualTo("username");
                ValueComparisonOperation<String> op =
                    new ValueComparisonOperation<>(
                        ValueComparisonOperator.EQ, new JsonLiteral<>("Aaron", JsonType.STRING));
                assertThat(
                        filterClause
                            .logicalExpression()
                            .comparisonExpressions
                            .get(0)
                            .getFilterOperations()
                            .get(0))
                    .isEqualTo(op);
              });
    }

    // Only "empty" Options allowed, nothing else
    @Test
    public void failForNonEmptyOptions() throws Exception {
      String json =
          """
                  {
                    "deleteOne": {
                      "filter" : {"_id" : "doc1"},
                      "options": {"setting":"abc"}
                    }
                  }
                  """;

      Exception e = catchException(() -> objectMapper.readValue(json, Command.class));
      assertThat(e)
          .isInstanceOf(JsonMappingException.class)
          .hasMessageStartingWith(
              ErrorCode.COMMAND_ACCEPTS_NO_OPTIONS.getMessage() + ": DeleteOneCommand");
    }
  }

  @Nested
  class CreateEmbeddingService {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
            {
              "createEmbeddingService": {
                "name": "openai",
                "apiProvider" : "openai",
                "apiKey" : "API-TOKEN",
                "baseUrl" : "https://api.openai.com/v1/"
              }
            }
            """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateEmbeddingServiceCommand.class,
              createEmbeddingServiceCommand -> {
                assertThat(createEmbeddingServiceCommand.name()).isNotNull();
                assertThat(createEmbeddingServiceCommand.apiKey()).isNotNull();
                assertThat(createEmbeddingServiceCommand.apiProvider()).isNotNull();
                assertThat(createEmbeddingServiceCommand.baseUrl()).isNotNull();
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
              "name": "some_name"
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

    @Test
    public void happyPathVectorSearch() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name": "some_name",
              "options": {
                "vector": {
                  "dimension": 5,
                  "metric": "cosine"
                }
              }
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
                assertThat(createCollection.options()).isNotNull();
                assertThat(createCollection.options().vector()).isNotNull();
                assertThat(createCollection.options().vector().dimension()).isEqualTo(5);
                assertThat(createCollection.options().vector().metric()).isEqualTo("cosine");
              });
    }

    @Test
    public void happyPathVectorSearchLegacyNames() throws Exception {
      Command result =
          objectMapper.readValue(
              """
                  {
                    "createCollection": {
                      "name": "some_name",
                      "options": {
                        "vector": {
                          "size": 5,
                          "function": "cosine"
                        }
                      }
                    }
                  }
                  """,
              Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionCommand.class,
              createCollection -> {
                assertThat(createCollection.name()).isNotNull();
                assertThat(createCollection.options()).isNotNull();
                assertThat(createCollection.options().vector()).isNotNull();
                assertThat(createCollection.options().vector().dimension()).isEqualTo(5);
                assertThat(createCollection.options().vector().metric()).isEqualTo("cosine");
              });
    }

    @Test
    public void happyPathVectorizeSearch() throws Exception {
      String json =
          """
              {
                "createCollection": {
                  "name": "some_name",
                  "options": {
                    "vector": {
                      "dimension": 5,
                      "metric": "cosine"
                    },
                    "vectorize" : {
                      "service" : "my_service",
                      "options" : {
                        "modelName": "text-embedding-ada-002"
                      }
                    }
                  }
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
                assertThat(createCollection.options()).isNotNull();
                assertThat(createCollection.options().vector()).isNotNull();
                assertThat(createCollection.options().vector().dimension()).isEqualTo(5);
                assertThat(createCollection.options().vector().metric()).isEqualTo("cosine");
                assertThat(createCollection.options().vectorize()).isNotNull();
                assertThat(createCollection.options().vectorize().service())
                    .isEqualTo("my_service");
                assertThat(createCollection.options().vectorize().options()).isNotNull();
                assertThat(createCollection.options().vectorize().options().modelName())
                    .isEqualTo("text-embedding-ada-002");
              });
    }

    @Test
    public void happyPathVectorSearchDefaultFunction() throws Exception {
      String json =
          """
          {
            "createCollection": {
              "name": "some_name",
              "options": {
                "vector": {
                  "dimension": 5
                }
              }
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
                assertThat(createCollection.options()).isNotNull();
                assertThat(createCollection.options().vector()).isNotNull();
                assertThat(createCollection.options().vector().dimension()).isEqualTo(5);
                assertThat(createCollection.options().vector().metric()).isEqualTo("cosine");
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

    @Test
    public void findOneAndUpdateWithOptions() throws Exception {
      String json =
          """
          {
            "findOneAndUpdate": {
                "filter" : {"username" : "update_user5"},
                "update" : {"$set" : {"new_col": {"sub_doc_col" : "new_val2"}}},
                "options" : {"returnDocument" : "after", "upsert" : true}
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
                assertThat(options.returnDocument()).isNotNull();
                assertThat(options.returnDocument()).isEqualTo("after");
                assertThat(options.upsert()).isTrue();
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
                "documents": [
                    {
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
                    }
                ],
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

  @Nested
  class Count {
    @Test
    public void happyPath() throws Exception {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"username" : "user1"}
            }
          }
          """;

      Command result = objectMapper.readValue(json, Command.class);
      assertThat(result)
          .isInstanceOfSatisfying(
              CountDocumentsCommand.class,
              countCommand -> {
                FilterClause filterClause = countCommand.filterClause();
                assertThat(filterClause).isNotNull();
              });
    }
  }
}
