package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingService;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOneAndReplaceCommandResolverTest {
  @Inject ObjectMapper objectMapper;
  @Inject OperationsConfig operationsConfig;
  @Inject Shredder shredder;
  @Inject FindOneAndReplaceCommandResolver resolver;

  @Nested
  class Resolve {

    CommandContext commandContext = CommandContext.empty();

    @Test
    public void idFilterCondition() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"_id" : "id"},
            "replacement" : {"col1" : "val1", "col2" : "val2"}
          }
        }
        """;
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";
      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.IDFilter filter =
                              new DBFilterBase.IDFilter(
                                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void idFilterConditionWithId() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"_id" : "id"},
            "replacement" : {"_id": "id", "col1" : "val1", "col2" : "val2"}
          }
        }
        """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";
      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"_id\": \"id\", \"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNotNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.IDFilter filter =
                              new DBFilterBase.IDFilter(
                                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void filterConditionSort() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"status" : "active"},
            "sort" : {"user" : 1},
            "replacement" : {"col1" : "val1", "col2" : "val2"}
          }
        }
        """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";
      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(1);
                          assertThat(find.orderBy())
                              .isEqualTo(List.of(new FindOperation.OrderBy("user", true)));
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void filterConditionVectorSearch() throws Exception {
      String json =
          """
                {
                  "findOneAndReplace": {
                    "filter" : {"status" : "active"},
                    "sort" : {"$vector" : [0.11,  0.22, 0.33, 0.44]},
                    "replacement" : {"col1" : "val1", "col2" : "val2"}
                  }
                }
                """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";
      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.11f, 0.22f, 0.33f, 0.44f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void filterConditionVectorizeSearch() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"status" : "active"},
            "sort" : {"$vectorize" : "test data"},
            "replacement" : {"col1" : "val1", "col2" : "val2", "$vectorize" : "test data"}
          }
        }
        """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation =
          resolver.resolveCommand(TestEmbeddingService.commandContextWithVectorize, command);
      String expected =
          "{\"col1\":\"val1\",\"col2\":\"val2\",\"$vectorize\":\"test data\",\"$vector\":[0.25,0.25,0.25]}";
      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext())
                    .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext())
                              .isEqualTo(TestEmbeddingService.commandContextWithVectorize);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.25f, 0.25f, 0.25f);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void idFilterConditionWithOptions() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"_id" : "id"},
            "replacement" : {"col1" : "val1", "col2" : "val2"},
            "options" : {"returnDocument" : "after" }
          }
        }
        """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.IDFilter filter =
                              new DBFilterBase.IDFilter(
                                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void idFilterConditionWithOptionsUpsert() throws Exception {
      String json =
          """
            {
              "findOneAndReplace": {
                "filter" : {"_id" : "id"},
                "replacement" : {"col1" : "val1", "col2" : "val2"},
                "options" : {"returnDocument" : "after", "upsert" : true }
              }
            }
            """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.upsert()).isTrue();
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.IDFilter filter =
                              new DBFilterBase.IDFilter(
                                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void filterConditionWithOptionsSort() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"age" : 35},
            "sort": { "user.name" : 1, "user.age" :  -1 },
            "replacement" : {"col1" : "val1", "col2" : "val2"},
            "options" : {"returnDocument" : "after"}
          }
        }
        """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.NumberFilter filter =
                              new DBFilterBase.NumberFilter(
                                  "age",
                                  DBFilterBase.MapFilterBase.Operator.EQ,
                                  new BigDecimal(35));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.SORTED_DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(2);
                          assertThat(find.orderBy())
                              .isEqualTo(
                                  List.of(
                                      new FindOperation.OrderBy("user.name", true),
                                      new FindOperation.OrderBy("user.age", false)));
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }

    @Test
    public void dynamicFilterCondition() throws Exception {
      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"col" : "val"},
            "replacement" : {"col1" : "val1", "col2" : "val2"}
          }
        }
        """;

      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Operation operation = resolver.resolveCommand(commandContext, command);
      String expected = "{\"col1\":\"val1\",\"col2\":\"val2\"}";

      assertThat(operation)
          .isInstanceOfSatisfying(
              ReadAndUpdateOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.shredder()).isEqualTo(shredder);
                assertThat(op.updateLimit()).isEqualTo(1);
                assertThat(op.retryLimit()).isEqualTo(operationsConfig.lwt().retries());
                assertThat(op.documentUpdater())
                    .isInstanceOfSatisfying(
                        DocumentUpdater.class,
                        replacer -> {
                          try {
                            ObjectNode replacement =
                                (ObjectNode)
                                    objectMapper.readTree(
                                        "{\"col1\" : \"val1\", \"col2\" : \"val2\"}");
                          } catch (JsonProcessingException e) {
                            e.printStackTrace();
                          }
                          assertThat(replacer.replaceDocument().toString()).isEqualTo(expected);
                          assertThat(replacer.replaceDocumentId()).isNull();
                        });
                assertThat(op.findOperation())
                    .isInstanceOfSatisfying(
                        FindOperation.class,
                        find -> {
                          DBFilterBase.TextFilter filter =
                              new DBFilterBase.TextFilter(
                                  "col", DBFilterBase.MapFilterBase.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(ReadType.DOCUMENT);
                          assertThat(
                                  find.logicalExpression()
                                      .comparisonExpressions
                                      .get(0)
                                      .getDbFilters()
                                      .get(0))
                              .isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }
  }
}
