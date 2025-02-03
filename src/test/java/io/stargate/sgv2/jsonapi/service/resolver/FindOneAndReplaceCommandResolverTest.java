package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.ReadAndUpdateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.MapCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.NumberCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.TextCollectionFilter;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
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
  @Inject DocumentShredder documentShredder;
  @Inject FindOneAndReplaceCommandResolver resolver;
  @InjectMock protected RequestContext dataApiRequestInfo;

  @Nested
  class Resolve {

    CommandContext<CollectionSchemaObject> commandContext = TestConstants.collectionContext();

    @Test
    public void invalidVectorizeUsage() throws Exception {

      String json =
          """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "id"},
                    "replacement" : {"$vectorize" : "vectorize text", "$vector" : [0.1,0.2]}
                  }
                }
                """;
      FindOneAndReplaceCommand command =
          objectMapper.readValue(json, FindOneAndReplaceCommand.class);
      Exception e = catchException(() -> resolver.resolveCommand(commandContext, command));
      assertThat(e)
          .isInstanceOf(JsonApiException.class)
          .hasMessageContaining("$vectorize` and `$vector` can't be used together");
    }

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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          IDCollectionFilter filter =
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          IDCollectionFilter filter =
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "status", MapCollectionFilter.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(1);
                          assertThat(find.orderBy())
                              .isEqualTo(
                                  List.of(new FindCollectionOperation.OrderBy("user", true)));
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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "status", MapCollectionFilter.Operator.EQ, "active");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.vector()).isNotNull();
                          assertThat(find.vector()).containsExactly(0.11f, 0.22f, 0.33f, 0.44f);
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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          IDCollectionFilter filter =
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.upsert()).isTrue();
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          IDCollectionFilter filter =
                              new IDCollectionFilter(
                                  IDCollectionFilter.Operator.EQ, DocumentId.fromString("id"));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isTrue();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          NumberCollectionFilter filter =
                              new NumberCollectionFilter(
                                  "age", MapCollectionFilter.Operator.EQ, new BigDecimal(35));

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageSize()).isEqualTo(100);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.SORTED_DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.orderBy()).hasSize(2);
                          assertThat(find.orderBy())
                              .isEqualTo(
                                  List.of(
                                      new FindCollectionOperation.OrderBy("user.name", true),
                                      new FindCollectionOperation.OrderBy("user.age", false)));
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
              ReadAndUpdateCollectionOperation.class,
              op -> {
                assertThat(op.commandContext()).isEqualTo(commandContext);
                assertThat(op.returnDocumentInResponse()).isTrue();
                assertThat(op.returnUpdatedDocument()).isFalse();
                assertThat(op.upsert()).isFalse();
                assertThat(op.documentShredder()).isEqualTo(documentShredder);
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
                assertThat(op.findCollectionOperation())
                    .isInstanceOfSatisfying(
                        FindCollectionOperation.class,
                        find -> {
                          TextCollectionFilter filter =
                              new TextCollectionFilter(
                                  "col", MapCollectionFilter.Operator.EQ, "val");

                          assertThat(find.objectMapper()).isEqualTo(objectMapper);
                          assertThat(find.commandContext()).isEqualTo(commandContext);
                          assertThat(find.pageSize()).isEqualTo(1);
                          assertThat(find.limit()).isEqualTo(1);
                          assertThat(find.pageState()).isNull();
                          assertThat(find.readType()).isEqualTo(CollectionReadType.DOCUMENT);
                          assertThat(find.dbLogicalExpression().filters().get(0)).isEqualTo(filter);
                          assertThat(find.singleResponse()).isTrue();
                        });
              });
    }
  }
}
