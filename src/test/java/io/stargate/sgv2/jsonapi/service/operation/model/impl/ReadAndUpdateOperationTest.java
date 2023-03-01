package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ReadAndUpdateOperationTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private CommandContext commandContext = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;
  @Inject QueryExecutor queryExecutor;

  @Nested
  class ReadAndUpdateOperationsTest {

    @Test
    public void findAndUpdate() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      UUID tx_id = UUID.randomUUID();
      String doc1 =
          """
                                {
                                      "_id": "doc1",
                                      "username": "user1"
                                    }
                                """;

      String doc1Updated =
          """
                                {
                                      "_id": "doc1",
                                      "username": "user1",
                                      "name" : "test"
                                    }
                                """;
      withQuery(
              collectionReadCql,
              Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
          .withPageSize(1)
          .withColumnSpec(
              List.of(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName("key")
                      .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                      .build(),
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName("tx_id")
                      .setType(TypeSpecs.UUID)
                      .build(),
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName("doc_json")
                      .setType(TypeSpecs.VARCHAR)
                      .build()))
          .returning(
              List.of(
                  List.of(
                      Values.of(
                          CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                      Values.of(tx_id),
                      Values.of(doc1))));

      String update =
          "UPDATE %s.%s "
              + "        SET"
              + "            tx_id = now(),"
              + "            exist_keys = ?,"
              + "            sub_doc_equals = ?,"
              + "            array_size = ?,"
              + "            array_equals = ?,"
              + "            array_contains = ?,"
              + "            query_bool_values = ?,"
              + "            query_dbl_values = ?,"
              + "            query_text_values = ?,"
              + "            query_null_values = ?,"
              + "            doc_json  = ?"
              + "        WHERE "
              + "            key = ?"
              + "        IF "
              + "            tx_id = ?";
      String collectionUpdateCql = update.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      final WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      withQuery(
              collectionUpdateCql,
              Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
              Values.of(CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
              Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
              Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
              Values.of(CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
              Values.of(
                  CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
              Values.of(
                  CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
              Values.of(CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
              Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
              Values.of(shredDocument.docJson()),
              Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
              Values.of(tx_id))
          .withColumnSpec(
              List.of(
                  QueryOuterClass.ColumnSpec.newBuilder()
                      .setName("applied")
                      .setType(TypeSpecs.BOOLEAN)
                      .build()))
          .returning(List.of(List.of(Values.of(true))));

      String updater =
          """
                        {
                          "findOneAndUpdate": {
                            "filter": {
                              "_id": "doc1"
                            },
                            "update": {
                              "$set": {
                                "name": "test"
                              }
                            }
                          }
                        }
                        """;

      FindOneAndUpdateCommand findOneAndUpdateCommand =
          objectMapper.readValue(updater, FindOneAndUpdateCommand.class);
      ReadOperation readOperation =
          new FindOperation(
              commandContext,
              List.of(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(findOneAndUpdateCommand.updateClause());
      ReadAndUpdateOperation operation =
          new ReadAndUpdateOperation(
              commandContext, readOperation, documentUpdater, true, false, false, shredder, 1);
      final Uni<Supplier<CommandResult>> execute = operation.execute(queryExecutor);
      final CommandResult commandResultSupplier =
          execute.subscribe().asCompletionStage().get().get();
      UniAssertSubscriber<Supplier<CommandResult>> subscriber =
          operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());
      assertThat(commandResultSupplier)
          .satisfies(
              commandResult -> {
                assertThat(commandResultSupplier.status()).isNotNull();
                assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                    .isEqualTo(1);
                assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                    .isEqualTo(1);
              });
    }
  }

  @Test
  public void findAndUpdateUpsert() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id = UUID.randomUUID();
    String doc1Updated =
        """
                            {
                                  "_id": "doc1",
                                  "name" : "test"
                                }
                            """;
    withQuery(
            collectionReadCql,
            Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
        .withPageSize(1)
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("key")
                    .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("tx_id")
                    .setType(TypeSpecs.UUID)
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("doc_json")
                    .setType(TypeSpecs.VARCHAR)
                    .build()))
        .returning(List.of());

    String update =
        "UPDATE %s.%s "
            + "        SET"
            + "            tx_id = now(),"
            + "            exist_keys = ?,"
            + "            sub_doc_equals = ?,"
            + "            array_size = ?,"
            + "            array_equals = ?,"
            + "            array_contains = ?,"
            + "            query_bool_values = ?,"
            + "            query_dbl_values = ?,"
            + "            query_text_values = ?,"
            + "            query_null_values = ?,"
            + "            doc_json  = ?"
            + "        WHERE "
            + "            key = ?"
            + "        IF "
            + "            tx_id = ?";
    String collectionUpdateCql = update.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    final JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    final WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    withQuery(
            collectionUpdateCql,
            Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
            Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
            Values.of(CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
            Values.of(CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
            Values.of(CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
            Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
            Values.of(shredDocument.docJson()),
            Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
            Values.NULL)
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("applied")
                    .setType(TypeSpecs.BOOLEAN)
                    .build()))
        .returning(List.of(List.of(Values.of(true))));

    String updater =
        """
                        {
                          "findOneAndUpdate": {
                            "filter": {
                              "_id": "doc1"
                            },
                            "update": {
                              "$set": {
                                "name": "test"
                              }
                            }
                          }
                        }
                        """;

    FindOneAndUpdateCommand findOneAndUpdateCommand =
        objectMapper.readValue(updater, FindOneAndUpdateCommand.class);
    ReadOperation readOperation =
        new FindOperation(
            commandContext,
            List.of(
                new DBFilterBase.IDFilter(
                    DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
            null,
            1,
            1,
            ReadType.DOCUMENT,
            objectMapper);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(findOneAndUpdateCommand.updateClause());
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            commandContext, readOperation, documentUpdater, true, false, true, shredder, 1);
    final Uni<Supplier<CommandResult>> execute = operation.execute(queryExecutor);
    final CommandResult commandResultSupplier = execute.subscribe().asCompletionStage().get().get();
    UniAssertSubscriber<Supplier<CommandResult>> subscriber =
        operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());
    assertThat(commandResultSupplier)
        .satisfies(
            commandResult -> {
              assertThat(commandResultSupplier.status()).isNotNull();
              assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                  .isEqualTo(0);
              assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                  .isEqualTo(0);
              assertThat((DocumentId) commandResultSupplier.status().get(CommandStatus.UPSERTED_ID))
                  .isEqualTo(new DocumentId.StringId("doc1"));
            });
  }

  @Test
  public void findAndUpdateNoData() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id = UUID.randomUUID();
    withQuery(
            collectionReadCql,
            Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
        .withPageSize(1)
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("key")
                    .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("tx_id")
                    .setType(TypeSpecs.UUID)
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("doc_json")
                    .setType(TypeSpecs.VARCHAR)
                    .build()))
        .returning(List.of());

    String updater =
        """
                        {
                          "findOneAndUpdate": {
                            "filter": {
                              "_id": "doc1"
                            },
                            "update": {
                              "$set": {
                                "name": "test"
                              }
                            }
                          }
                        }
                        """;

    FindOneAndUpdateCommand findOneAndUpdateCommand =
        objectMapper.readValue(updater, FindOneAndUpdateCommand.class);
    ReadOperation readOperation =
        new FindOperation(
            commandContext,
            List.of(
                new DBFilterBase.IDFilter(
                    DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
            null,
            1,
            1,
            ReadType.DOCUMENT,
            objectMapper);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(findOneAndUpdateCommand.updateClause());
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            commandContext, readOperation, documentUpdater, true, false, false, shredder, 1);
    final Uni<Supplier<CommandResult>> execute = operation.execute(queryExecutor);
    final CommandResult commandResultSupplier = execute.subscribe().asCompletionStage().get().get();
    UniAssertSubscriber<Supplier<CommandResult>> subscriber =
        operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());
    assertThat(commandResultSupplier)
        .satisfies(
            commandResult -> {
              assertThat(commandResultSupplier.status()).isNotNull();
              assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                  .isEqualTo(0);
              assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                  .isEqualTo(0);
            });
  }

  @Test
  public void findAndUpdateMany() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 21"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    String doc1 =
        """
                                  {
                                        "_id": "doc1",
                                        "username": "user1",
                                        "status" : "active"
                                      }
                                  """;

    String doc1Updated =
        """
                                  {
                                        "_id": "doc1",
                                        "username": "user1",
                                        "status" : "active",
                                        "name" : "test"
                                      }
                                  """;

    String doc2 =
        """
                                  {
                                        "_id": "doc2",
                                        "username": "user2",
                                        "status" : "active"
                                      }
                                  """;

    String doc2Updated =
        """
                                  {
                                        "_id": "doc2",
                                        "username": "user2",
                                        "status" : "active",
                                        "name" : "test"
                                      }
                                  """;
    withQuery(collectionReadCql, Values.of("status Sactive"))
        .withPageSize(20)
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("key")
                    .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("tx_id")
                    .setType(TypeSpecs.UUID)
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("doc_json")
                    .setType(TypeSpecs.VARCHAR)
                    .build()))
        .returning(
            List.of(
                List.of(
                    Values.of(
                        CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                    Values.of(tx_id1),
                    Values.of(doc1)),
                List.of(
                    Values.of(
                        CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                    Values.of(tx_id2),
                    Values.of(doc2))));

    String update =
        "UPDATE %s.%s "
            + "        SET"
            + "            tx_id = now(),"
            + "            exist_keys = ?,"
            + "            sub_doc_equals = ?,"
            + "            array_size = ?,"
            + "            array_equals = ?,"
            + "            array_contains = ?,"
            + "            query_bool_values = ?,"
            + "            query_dbl_values = ?,"
            + "            query_text_values = ?,"
            + "            query_null_values = ?,"
            + "            doc_json  = ?"
            + "        WHERE "
            + "            key = ?"
            + "        IF "
            + "            tx_id = ?";
    String collectionUpdateCql = update.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    withQuery(
            collectionUpdateCql,
            Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
            Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
            Values.of(CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
            Values.of(CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
            Values.of(CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
            Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
            Values.of(shredDocument.docJson()),
            Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
            Values.of(tx_id1))
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("applied")
                    .setType(TypeSpecs.BOOLEAN)
                    .build()))
        .returning(List.of(List.of(Values.of(true))));

    jsonNode = objectMapper.readTree(doc2Updated);
    shredDocument = shredder.shred(jsonNode);

    withQuery(
            collectionUpdateCql,
            Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
            Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
            Values.of(CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
            Values.of(CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
            Values.of(CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
            Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
            Values.of(shredDocument.docJson()),
            Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
            Values.of(tx_id2))
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("applied")
                    .setType(TypeSpecs.BOOLEAN)
                    .build()))
        .returning(List.of(List.of(Values.of(true))));

    String updater =
        """
                          {
                            "findOneAndUpdate": {
                              "filter": {
                                "_id": "doc1"
                              },
                              "update": {
                                "$set": {
                                  "name": "test"
                                }
                              }
                            }
                          }
                          """;

    FindOneAndUpdateCommand findOneAndUpdateCommand =
        objectMapper.readValue(updater, FindOneAndUpdateCommand.class);
    ReadOperation readOperation =
        new FindOperation(
            commandContext,
            List.of(
                new DBFilterBase.TextFilter(
                    "status", DBFilterBase.MapFilterBase.Operator.EQ, "active")),
            null,
            21,
            20,
            ReadType.DOCUMENT,
            objectMapper);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(findOneAndUpdateCommand.updateClause());
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            commandContext, readOperation, documentUpdater, true, false, false, shredder, 20);
    final Uni<Supplier<CommandResult>> execute = operation.execute(queryExecutor);
    final CommandResult commandResultSupplier = execute.subscribe().asCompletionStage().get().get();
    UniAssertSubscriber<Supplier<CommandResult>> subscriber =
        operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());
    assertThat(commandResultSupplier)
        .satisfies(
            commandResult -> {
              assertThat(commandResultSupplier.status()).isNotNull();
              assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                  .isEqualTo(2);
              assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                  .isEqualTo(2);
            });
  }

  @Test
  public void findAndUpdateManyUpsert() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 21"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id = UUID.randomUUID();
    String doc1Updated =
        """
                                {
                                      "_id": "doc1",
                                      "name" : "test"
                                    }
                                """;
    withQuery(
            collectionReadCql,
            Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
        .withPageSize(20)
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("key")
                    .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("tx_id")
                    .setType(TypeSpecs.UUID)
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("doc_json")
                    .setType(TypeSpecs.VARCHAR)
                    .build()))
        .returning(List.of());

    String update =
        "UPDATE %s.%s "
            + "        SET"
            + "            tx_id = now(),"
            + "            exist_keys = ?,"
            + "            sub_doc_equals = ?,"
            + "            array_size = ?,"
            + "            array_equals = ?,"
            + "            array_contains = ?,"
            + "            query_bool_values = ?,"
            + "            query_dbl_values = ?,"
            + "            query_text_values = ?,"
            + "            query_null_values = ?,"
            + "            doc_json  = ?"
            + "        WHERE "
            + "            key = ?"
            + "        IF "
            + "            tx_id = ?";
    String collectionUpdateCql = update.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    final JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    final WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    withQuery(
            collectionUpdateCql,
            Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
            Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
            Values.of(CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
            Values.of(CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
            Values.of(CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
            Values.of(CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
            Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
            Values.of(shredDocument.docJson()),
            Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
            Values.NULL)
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("applied")
                    .setType(TypeSpecs.BOOLEAN)
                    .build()))
        .returning(List.of(List.of(Values.of(true))));

    String updater =
        """
                            {
                              "findOneAndUpdate": {
                                "filter": {
                                  "_id": "doc1"
                                },
                                "update": {
                                  "$set": {
                                    "name": "test"
                                  }
                                }
                              }
                            }
                            """;

    FindOneAndUpdateCommand findOneAndUpdateCommand =
        objectMapper.readValue(updater, FindOneAndUpdateCommand.class);
    ReadOperation readOperation =
        new FindOperation(
            commandContext,
            List.of(
                new DBFilterBase.IDFilter(
                    DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
            null,
            21,
            20,
            ReadType.DOCUMENT,
            objectMapper);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(findOneAndUpdateCommand.updateClause());
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            commandContext, readOperation, documentUpdater, true, false, true, shredder, 20);
    final Uni<Supplier<CommandResult>> execute = operation.execute(queryExecutor);
    final CommandResult commandResultSupplier = execute.subscribe().asCompletionStage().get().get();
    UniAssertSubscriber<Supplier<CommandResult>> subscriber =
        operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());
    assertThat(commandResultSupplier)
        .satisfies(
            commandResult -> {
              assertThat(commandResultSupplier.status()).isNotNull();
              assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                  .isEqualTo(0);
              assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                  .isEqualTo(0);
              assertThat((DocumentId) commandResultSupplier.status().get(CommandStatus.UPSERTED_ID))
                  .isEqualTo(new DocumentId.StringId("doc1"));
            });
  }

  @Test
  public void findAndUpdateManyNoData() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 21"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id = UUID.randomUUID();
    withQuery(
            collectionReadCql,
            Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
        .withPageSize(20)
        .withColumnSpec(
            List.of(
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("key")
                    .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("tx_id")
                    .setType(TypeSpecs.UUID)
                    .build(),
                QueryOuterClass.ColumnSpec.newBuilder()
                    .setName("doc_json")
                    .setType(TypeSpecs.VARCHAR)
                    .build()))
        .returning(List.of());

    String updater =
        """
                            {
                              "findOneAndUpdate": {
                                "filter": {
                                  "_id": "doc1"
                                },
                                "update": {
                                  "$set": {
                                    "name": "test"
                                  }
                                }
                              }
                            }
                            """;

    FindOneAndUpdateCommand findOneAndUpdateCommand =
        objectMapper.readValue(updater, FindOneAndUpdateCommand.class);
    ReadOperation readOperation =
        new FindOperation(
            commandContext,
            List.of(
                new DBFilterBase.IDFilter(
                    DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"))),
            null,
            21,
            20,
            ReadType.DOCUMENT,
            objectMapper);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(findOneAndUpdateCommand.updateClause());
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            commandContext, readOperation, documentUpdater, true, false, false, shredder, 20);
    final Uni<Supplier<CommandResult>> execute = operation.execute(queryExecutor);
    final CommandResult commandResultSupplier = execute.subscribe().asCompletionStage().get().get();
    UniAssertSubscriber<Supplier<CommandResult>> subscriber =
        operation.execute(queryExecutor).subscribe().withSubscriber(UniAssertSubscriber.create());
    assertThat(commandResultSupplier)
        .satisfies(
            commandResult -> {
              assertThat(commandResultSupplier.status()).isNotNull();
              assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                  .isEqualTo(0);
              assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                  .isEqualTo(0);
            });
  }
}
