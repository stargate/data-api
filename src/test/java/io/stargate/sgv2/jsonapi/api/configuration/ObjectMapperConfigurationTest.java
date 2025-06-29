package io.stargate.sgv2.jsonapi.api.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.datastax.oss.driver.api.core.data.CqlVector;
import com.fasterxml.jackson.databind.*;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableOperation;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableOperationImpl;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class ObjectMapperConfigurationTest {

  @Inject ObjectMapper objectMapper;

  @Inject DocumentLimitsConfig documentLimitsConfig;

  private final TestConstants testConstants = new TestConstants();

  @Nested
  class UnmatchedOperationCommandHandlerTest {
    @Test
    public void notExistedCommandMatchKeyspaceCommand() throws Exception {
      String json =
          """
                    {
                      "notExistedCommand": {
                      }
                    }
                    """;
      Exception e = catchException(() -> objectMapper.readValue(json, KeyspaceCommand.class));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(
              "Provided command unknown: \"notExistedCommand\" not one of \"KeyspaceCommand\"s");
    }

    @Test
    public void collectionCommandNotMatchKeyspaceCommand() throws Exception {
      String json =
          """
                            {
                              "find": {
                              }
                            }
                            """;
      Exception e = catchException(() -> objectMapper.readValue(json, KeyspaceCommand.class));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(
              "Provided command unknown: \"find\" not one of \"KeyspaceCommand\"s");
    }

    @Test
    public void collectionCommandNotMatchGeneralCommand() throws Exception {
      String json =
          """
                            {
                              "insertOne": {
                              }
                            }
                            """;
      Exception e = catchException(() -> objectMapper.readValue(json, GeneralCommand.class));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(
              "Provided command unknown: \"insertOne\" not one of \"GeneralCommand\"s");
    }

    @Test
    public void generalCommandNotMatchCollectionCommand() throws Exception {
      String json =
          """
                                  {
                                    "createKeyspace": {
                                    }
                                  }
                                  """;
      Exception e = catchException(() -> objectMapper.readValue(json, CollectionCommand.class));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(
              "Provided command unknown: \"createKeyspace\" not one of \"CollectionCommand\"s");

      String deprecatedCommandJson =
          """
                            {
                              "createNamespace": {
                              }
                            }
                            """;
      Exception e1 =
          catchException(
              () -> objectMapper.readValue(deprecatedCommandJson, CollectionCommand.class));
      assertThat(e1)
          .isInstanceOf(JsonApiException.class)
          .hasMessageStartingWith(
              "Provided command unknown: \"createNamespace\" not one of \"CollectionCommand\"s");
    }
  }

  // Tests for CQL value serialization handling
  @Nested
  class ValueHandlingCqlSerialization {
    // [data-api#1698] Default CqlVector serialization not what we need for InsertedIds
    @Test
    public void okCqlVectorSerialization() throws Exception {
      CqlVector<Float> input = CqlVector.newInstance(1.0f, 0.5f, -0.25f, 0.0f);
      assertThat(objectMapper.writeValueAsString(input)).isEqualTo("[1.0,0.5,-0.25,0.0]");
    }
  }

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
                SortClause sortClause = findOne.sortClause(testConstants.collectionContext());
                assertThat(sortClause).isNotNull();
                assertThat(sortClause.sortExpressions())
                    .contains(
                        SortExpression.sort("user.name", true),
                        SortExpression.sort("user.age", false));

                FilterClause filterClause =
                    filterClause(testConstants.collectionContext(), findOne);
                assertThat(filterClause).isNotNull();
                assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount())
                    .isEqualTo(1);
                assertThat(filterClause.logicalExpression().comparisonExpressions)
                    .singleElement()
                    .satisfies(
                        expression -> {
                          ValueComparisonOperation<?> op =
                              ValueComparisonOperation.build(ValueComparisonOperator.EQ, "aaron");

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
              findOne -> {
                SortClause sc = findOne.sortClause(testConstants.collectionContext());
                Assertions.assertThat(sc).isNotNull();
                Assertions.assertThat(sc.isEmpty()).isTrue();
              });
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
              findOne -> Assertions.assertThat(findOne.filterDefinition()).isNull());
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
              ErrorCodeV1.COMMAND_ACCEPTS_NO_OPTIONS.getMessage() + ": `InsertOneCommand`");
    }

    @Test
    public void failForTooLongNumbers() {
      String tooLongNumStr = "1234567890".repeat(11);
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
              "Number value length ("
                  + tooLongNumStr.length()
                  + ") exceeds the maximum allowed ("
                  + documentLimitsConfig.maxNumberLength());
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
                FilterClause filterClause = filterClause(testConstants.collectionContext(), cmd);
                assertThat(filterClause).isNotNull();
                assertThat(filterClause.logicalExpression().getTotalComparisonExpressionCount())
                    .isEqualTo(1);

                assertThat(filterClause.logicalExpression().comparisonExpressions.get(0).getPath())
                    .isEqualTo("username");
                ValueComparisonOperation<?> op =
                    ValueComparisonOperation.build(ValueComparisonOperator.EQ, "Aaron");
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
              ErrorCodeV1.COMMAND_ACCEPTS_NO_OPTIONS.getMessage() + ": `DeleteOneCommand`");
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
                                    "metric": "cosine",
                                    "dimension": 1536,
                                    "service": {
                                        "provider": "openai",
                                        "modelName": "text-embedding-ada-002",
                                        "parameters": {
                                            "projectId": "test project"
                                        }
                                    }
                                },
                                "indexing": {
                                    "deny": [
                                        "address"
                                    ]
                                }
                            }
                        }
                    }
                        """;

      Command result = objectMapper.readValue(json, Command.class);

      Map<String, Object> parameterMap = new HashMap<>();
      parameterMap.put("projectId", "test project");

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateCollectionCommand.class,
              createCollection -> {
                String name = createCollection.name();
                assertThat(name).isNotNull();
                assertThat(createCollection.options()).isNotNull();
                assertThat(createCollection.options().vector()).isNotNull();
                assertThat(createCollection.options().vector().dimension()).isEqualTo(1536);
                assertThat(createCollection.options().vector().metric()).isEqualTo("cosine");
                assertThat(createCollection.options().vector().vectorizeConfig()).isNotNull();
                assertThat(createCollection.options().vector().vectorizeConfig().provider())
                    .isEqualTo("openai");
                assertThat(createCollection.options().vector().vectorizeConfig().modelName())
                    .isEqualTo("text-embedding-ada-002");
                assertThat(createCollection.options().vector().vectorizeConfig().parameters())
                    .isNotNull();
                assertThat(createCollection.options().vector().vectorizeConfig().parameters())
                    .isEqualTo(parameterMap);
                assertThat(createCollection.options().indexing()).isNotNull();
                assertThat(createCollection.options().indexing().allow()).isNull();
                assertThat(createCollection.options().indexing().deny()).hasSize(1);
                assertThat(createCollection.options().indexing().deny()).contains("address");
              });
    }

    @Test
    public void happyPathIndexingAllow() throws Exception {
      String json =
          """
                        {
                          "createCollection": {
                            "name": "some_name",
                            "options": {
                              "indexing": {
                                "allow": ["field1", "field2"]
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
                assertThat(createCollection.options().indexing()).isNotNull();
                assertThat(createCollection.options().indexing().deny()).isNull();
                assertThat(createCollection.options().indexing().allow()).hasSize(2);
                assertThat(createCollection.options().indexing().allow())
                    .contains("field1", "field2");
              });
    }

    @Test
    public void happyPathIndexingDeny() throws Exception {
      String json =
          """
                            {
                              "createCollection": {
                                "name": "some_name",
                                "options": {
                                  "indexing": {
                                    "deny": ["field1", "field2"]
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
                assertThat(createCollection.options().indexing()).isNotNull();
                assertThat(createCollection.options().indexing().allow()).isNull();
                assertThat(createCollection.options().indexing().deny()).hasSize(2);
                assertThat(createCollection.options().indexing().deny())
                    .contains("field1", "field2");
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
                assertThat(createCollection.options().vector().metric()).isNull();
                assertThat(createCollection.options().vector().sourceModel()).isNull();
              });
    }
  }

  @Nested
  class CreateTable {
    @Test
    public void happyPathWithOption() throws Exception {
      String json =
          """
                            {
                                 "createTable": {
                                     "name": "person",
                                     "definition": {
                                         "columns": {
                                             "id": "text",
                                             "age": "int",
                                             "name": "text",
                                             "city": "text"
                                         },
                                         "primaryKey": "id"
                                     },
                                     "options" : {
                                         "ifNotExists" : true
                                     }
                                 }
                             }
                            """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateTableCommand.class,
              createTableCommand -> {
                String name = createTableCommand.name();
                assertThat(name).isNotNull();
                assertThat(createTableCommand.options().ifNotExists()).isTrue();
              });
    }

    @Test
    public void happyPathWithEmptyOption() throws Exception {
      String json =
          """
                                    {
                                         "createTable": {
                                             "name": "person",
                                             "definition": {
                                                 "columns": {
                                                     "id": "text",
                                                     "age": "int",
                                                     "name": "text",
                                                     "city": "text"
                                                 },
                                                 "primaryKey": "id"
                                             },
                                             "options" : {}
                                         }
                                     }
                                    """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateTableCommand.class,
              createTableCommand -> {
                String name = createTableCommand.name();
                assertThat(name).isNotNull();
                assertThat(createTableCommand.options()).isNotNull();
                assertThat(createTableCommand.options().ifNotExists()).isNull();
              });
    }
  }

  @Nested
  class CreateIndex {
    @Test
    public void happyPathWithOption() throws Exception {
      String json =
          """
                            {
                                  "createIndex": {
                                      "name": "name_idx",
                                      "definition": {
                                          "column": "name",
                                          "options": {
                                              "caseSensitive": false,
                                              "normalize": true,
                                              "ascii": true
                                          }
                                      },
                                      "options" : {
                                         "ifNotExists" : true
                                      }
                                  }
                              }
                                      """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateIndexCommand.class,
              createIndexCommand -> {
                String name = createIndexCommand.name();
                assertThat(name).isNotNull();
                assertThat(createIndexCommand.definition().column()).isNotNull();
                assertThat(createIndexCommand.options().ifNotExists()).isTrue();
                assertThat(createIndexCommand.definition().options().caseSensitive()).isFalse();
                assertThat(createIndexCommand.definition().options().normalize()).isTrue();
                assertThat(createIndexCommand.definition().options().ascii()).isTrue();
              });
    }

    @Test
    public void happyPathWithEmptyOption() throws Exception {
      String json =
          """
                            {
                                  "createIndex": {
                                      "name": "name_idx",
                                      "definition": {
                                          "column": "name",
                                          "options": {
                                              "caseSensitive": false,
                                              "normalize": true,
                                              "ascii": true
                                          }
                                      },
                                      "options" : {}
                                  }
                              }
                                      """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              CreateIndexCommand.class,
              createIndexCommand -> {
                String name = createIndexCommand.name();
                assertThat(name).isNotNull();
                assertThat(createIndexCommand.definition().column()).isNotNull();
                assertThat(createIndexCommand.options().ifNotExists()).isNull();
                assertThat(createIndexCommand.definition().options().caseSensitive()).isFalse();
                assertThat(createIndexCommand.definition().options().normalize()).isTrue();
                assertThat(createIndexCommand.definition().options().ascii()).isTrue();
              });
    }
  }

  @Nested
  class DropTable {
    @Test
    public void happyPathWithOption() throws Exception {
      String json =
          """
                            {
                                   "dropTable": {
                                       "name": "allTypesTable",
                                       "options" : {
                                          "ifExists" : true
                                       }
                                   }
                               }
                                                """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              DropTableCommand.class,
              dropTableCommand -> {
                String name = dropTableCommand.name();
                assertThat(name).isNotNull();
                assertThat(dropTableCommand.options().ifExists()).isTrue();
              });
    }

    @Test
    public void happyPathWithEmptyOption() throws Exception {
      String json =
          """
                       {
                                   "dropTable": {
                                       "name": "allTypesTable",
                                       "options" : {}
                                   }
                               }
                               """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              DropTableCommand.class,
              dropTableCommand -> {
                String name = dropTableCommand.name();
                assertThat(name).isNotNull();
                assertThat(dropTableCommand.options().ifExists()).isNull();
              });
    }
  }

  @Nested
  class DropIndex {
    @Test
    public void happyPathWithOption() throws Exception {
      String json =
          """
                                      {
                                             "dropIndex": {
                                                 "name": "text_index",
                                                 "options" : {
                                                    "ifExists" : true
                                                 }
                                             }
                                         }
                                                          """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              DropIndexCommand.class,
              dropIndexCommand -> {
                String name = dropIndexCommand.name();
                assertThat(name).isNotNull();
                assertThat(dropIndexCommand.options().ifExists()).isTrue();
              });
    }

    @Test
    public void happyPathWithEmptyOption() throws Exception {
      String json =
          """
                                 {
                                             "dropIndex": {
                                                 "name": "text_index",
                                                 "options" : {}
                                             }
                                         }
                                         """;

      Command result = objectMapper.readValue(json, Command.class);

      assertThat(result)
          .isInstanceOfSatisfying(
              DropIndexCommand.class,
              dropIndexCommand -> {
                String name = dropIndexCommand.name();
                assertThat(name).isNotNull();
                assertThat(dropIndexCommand.options().ifExists()).isNull();
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
                FilterDefinition filterSpec = findOneAndUpdateCommand.filterDefinition();
                assertThat(filterSpec).isNotNull();
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
                FilterDefinition filterSpec = findOneAndUpdateCommand.filterDefinition();
                assertThat(filterSpec).isNotNull();
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
                assertThat(options.ordered()).isFalse();
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
                FilterDefinition filterSpec = countCommand.filterDefinition();
                assertThat(filterSpec).isNotNull();
              });
    }
  }

  @Nested
  class AlterTable {
    @Test
    public void addColumns() throws Exception {
      String json =
          """
                              {
                                "alterTable": {
                                    "operation": {
                                        "add": {
                                            "columns": {
                                                "new_col_1" :"text",
                                                "new_col_2" : {
                                                    "type": "map",
                                                    "keyType": "text",
                                                    "valueType": "text"
                                                },
                                                "content": {
                                                 "type": "vector",
                                                 "dimension": 1024,
                                                 "service": {
                                                   "provider": "nvidia",
                                                   "modelName": "NV-Embed-QA"
                                                 }
                                               },
                                               "vector_1": {
                                                 "type": "vector",
                                                 "service": {
                                                   "provider": "nvidia",
                                                   "modelName": "NV-Embed-QA"
                                                 }
                                               }
                                            }
                                        }
                                    }
                                }
                              }
                              """;

      Command result = objectMapper.readValue(json, Command.class);
      assertThat(result)
          .isInstanceOfSatisfying(
              AlterTableCommand.class,
              alterTableCommand -> {
                AlterTableOperation operation = alterTableCommand.operation();
                assertThat(operation).isNotNull();
                assertThat(operation)
                    .isInstanceOfSatisfying(
                        AlterTableOperationImpl.AddColumns.class,
                        addColumns -> {
                          Map<String, ColumnDesc> columns = addColumns.columns();
                          assertThat(columns).isNotNull();
                          assertThat(columns).hasSize(4);
                          assertThat(columns).containsEntry("new_col_1", PrimitiveColumnDesc.TEXT);
                          assertThat(columns)
                              .containsEntry(
                                  "new_col_2",
                                  new MapColumnDesc(
                                      PrimitiveColumnDesc.TEXT, PrimitiveColumnDesc.TEXT));
                          assertThat(columns)
                              .containsEntry(
                                  "content",
                                  new VectorColumnDesc(
                                      1024,
                                      new VectorizeConfig("nvidia", "NV-Embed-QA", null, null)));
                          assertThat(columns)
                              .containsEntry(
                                  "vector_1",
                                  new VectorColumnDesc(
                                      null,
                                      new VectorizeConfig("nvidia", "NV-Embed-QA", null, null)));
                        });
              });
    }

    @Test
    public void dropColumns() throws Exception {
      String json =
          """
                                      {
                                        "alterTable": {
                                            "operation": {
                                                "drop": {
                                                   "columns": ["new_col_1", "new_col_2"]
                                                }
                                            }
                                        }
                                      }
                                      """;

      Command result = objectMapper.readValue(json, Command.class);
      assertThat(result)
          .isInstanceOfSatisfying(
              AlterTableCommand.class,
              alterTableCommand -> {
                AlterTableOperation operation = alterTableCommand.operation();
                assertThat(operation).isNotNull();
                assertThat(operation)
                    .isInstanceOfSatisfying(
                        AlterTableOperationImpl.DropColumns.class,
                        dropColumns -> {
                          List<String> columns = dropColumns.columns();
                          assertThat(columns).isNotNull();
                          assertThat(columns).hasSize(2);
                          assertThat(columns).contains("new_col_1", "new_col_2");
                        });
              });
    }

    @Test
    public void addVectorize() throws Exception {
      String json =
          """
                      {
                        "alterTable": {
                            "operation": {
                                "addVectorize": {
                                    "columns": {
                                        "vector_column_1" : {
                                            "provider": "nvidia",
                                            "modelName": "NV-Embed-QA"
                                        },
                                        "vector_column_2" : {
                                            "provider": "mistral",
                                            "modelName": "mistral-embed"
                                        }
                                    }
                                }
                            }
                        }
                      }
                      """;

      Command result = objectMapper.readValue(json, Command.class);
      assertThat(result)
          .isInstanceOfSatisfying(
              AlterTableCommand.class,
              alterTableCommand -> {
                AlterTableOperation operation = alterTableCommand.operation();
                assertThat(operation).isNotNull();
                assertThat(operation)
                    .isInstanceOfSatisfying(
                        AlterTableOperationImpl.AddVectorize.class,
                        addVectorizeConfig -> {
                          Map<String, VectorizeConfig> columns = addVectorizeConfig.columns();
                          assertThat(columns).isNotNull();
                          assertThat(columns).hasSize(2);
                          assertThat(columns)
                              .containsEntry(
                                  "vector_column_1",
                                  new VectorizeConfig("nvidia", "NV-Embed-QA", null, null));
                          assertThat(columns)
                              .containsEntry(
                                  "vector_column_2",
                                  new VectorizeConfig("mistral", "mistral-embed", null, null));
                        });
              });
    }

    @Test
    public void dropVectorize() throws Exception {
      String json =
          """
              {
                "alterTable": {
                    "operation": {
                        "dropVectorize": {
                           "columns": ["vector_column_1"]
                        }
                    }
                }
              }
              """;

      Command result = objectMapper.readValue(json, Command.class);
      assertThat(result)
          .isInstanceOfSatisfying(
              AlterTableCommand.class,
              alterTableCommand -> {
                AlterTableOperation operation = alterTableCommand.operation();
                assertThat(operation).isNotNull();
                assertThat(operation)
                    .isInstanceOfSatisfying(
                        AlterTableOperationImpl.DropVectorize.class,
                        dropVectorizeForColumns -> {
                          List<String> columns = dropVectorizeForColumns.columns();
                          assertThat(columns).isNotNull();
                          assertThat(columns).hasSize(1);
                          assertThat(columns).contains("vector_column_1");
                        });
              });
    }
  }

  private <CMD extends Command & Filterable> FilterClause filterClause(
      CommandContext<?> ctx, CMD cmd) {
    return cmd.filterClause(ctx);
  }
}
