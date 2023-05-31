package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ReadAndUpdateOperationRetryTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final CommandContext COMMAND_CONTEXT =
      new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;
  @Inject QueryExecutor queryExecutor;
  @Inject QueriesConfig queriesConfig;

  // TODO: as part of https://github.com/stargate/jsonapi/issues/214
  //  - non-lwt failure partial, full
  //  - non-lwt failure on retry
  private static String UPDATE =
      "UPDATE \"%s\".\"%s\" "
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
          + "            query_timestamp_values = ?,"
          + "            doc_json  = ?"
          + "        WHERE "
          + "            key = ?"
          + "        IF "
          + "            tx_id = ?";

  @Test
  public void findOneAndUpdateWithRetry() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
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
    ValidatingStargateBridge.QueryAssert selectQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("username " + new DocValueHasher().getHash("user1").hash()))
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id1),
                        Values.of(doc1))));

    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND key = ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    ValidatingStargateBridge.QueryAssert reReadQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("username " + new DocValueHasher().getHash("user1").hash()),
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id2),
                        Values.of(doc1))));

    String collectionUpdateCql = UPDATE.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert failedUpdateQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id1))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            // `false` in response for LWT indicate failure
            .returning(List.of(List.of(Values.of(false))));

    ValidatingStargateBridge.QueryAssert updateQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id2))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            .returning(List.of(List.of(Values.of(true))));

    DBFilterBase.TextFilter filter =
        new DBFilterBase.TextFilter("username", DBFilterBase.MapFilterBase.Operator.EQ, "user1");
    FindOperation findOperation =
        new FindOperation(
            COMMAND_CONTEXT,
            List.of(filter),
            DocumentProjector.identityProjector(),
            null,
            1,
            1,
            ReadType.DOCUMENT,
            objectMapper,
            null,
            0,
            0,
            false);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            1,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    selectQueryAssert.assertExecuteCount().isOne();
    reReadQueryAssert.assertExecuteCount().isOne();
    failedUpdateQueryAssert.assertExecuteCount().isOne();
    updateQueryAssert.assertExecuteCount().isOne();

    // then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 1)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
    assertThat(result.errors()).isNull();
  }

  @Test
  public void findAndUpdateWithRetryFailure() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
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
    ValidatingStargateBridge.QueryAssert selectQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("username " + new DocValueHasher().getHash("user1").hash()))
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id1),
                        Values.of(doc1))));

    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND key = ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    ValidatingStargateBridge.QueryAssert reReadQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("username " + new DocValueHasher().getHash("user1").hash()),
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id2),
                        Values.of(doc1))));

    String collectionUpdateCql = UPDATE.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert updateFailedQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id1))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            // `false` in response for LWT indicate failure
            .returning(List.of(List.of(Values.of(false))));

    ValidatingStargateBridge.QueryAssert updateRetryFailedQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id2))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            .returning(List.of(List.of(Values.of(false))));

    DBFilterBase.TextFilter filter =
        new DBFilterBase.TextFilter("username", DBFilterBase.MapFilterBase.Operator.EQ, "user1");
    FindOperation findOperation =
        new FindOperation(
            COMMAND_CONTEXT,
            List.of(filter),
            DocumentProjector.identityProjector(),
            null,
            1,
            1,
            ReadType.DOCUMENT,
            objectMapper,
            null,
            0,
            0,
            false);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            1,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    selectQueryAssert.assertExecuteCount().isOne();
    reReadQueryAssert.assertExecuteCount().isEqualTo(3);
    updateFailedQueryAssert.assertExecuteCount().isOne();
    updateRetryFailedQueryAssert.assertExecuteCount().isEqualTo(3);

    // then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 1)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 0);
    assertThat(result.errors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.fields()).containsEntry("errorCode", "CONCURRENCY_FAILURE");
              assertThat(error.message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
            });
  }

  @Test
  public void findAndUpdateWithRetryFailureWithUpsert() throws Exception {
    // ensure document is not created
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
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
    ValidatingStargateBridge.QueryAssert selectQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("username " + new DocValueHasher().getHash("user1").hash()))
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id1),
                        Values.of(doc1))));

    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND key = ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    ValidatingStargateBridge.QueryAssert reReadQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("username " + new DocValueHasher().getHash("user1").hash()),
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id2),
                        Values.of(doc1))));

    String collectionUpdateCql = UPDATE.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert updateFailedQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id1))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            // `false` in response for LWT indicate failure
            .returning(List.of(List.of(Values.of(false))));

    ValidatingStargateBridge.QueryAssert updateRetryFailedQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id2))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            .returning(List.of(List.of(Values.of(false))));

    DBFilterBase.TextFilter filter =
        new DBFilterBase.TextFilter("username", DBFilterBase.MapFilterBase.Operator.EQ, "user1");
    FindOperation findOperation =
        new FindOperation(
            COMMAND_CONTEXT,
            List.of(filter),
            DocumentProjector.identityProjector(),
            null,
            1,
            1,
            ReadType.DOCUMENT,
            objectMapper,
            null,
            0,
            0,
            false);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            true,
            shredder,
            DocumentProjector.identityProjector(),
            1,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    selectQueryAssert.assertExecuteCount().isOne();
    reReadQueryAssert.assertExecuteCount().isEqualTo(3);
    updateFailedQueryAssert.assertExecuteCount().isOne();
    updateRetryFailedQueryAssert.assertExecuteCount().isEqualTo(3);

    // then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 1)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 0);
    assertThat(result.errors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.fields()).containsEntry("errorCode", "CONCURRENCY_FAILURE");
              assertThat(error.message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
            });
  }

  @Test
  public void findAndUpdateWithRetryPartialFailure() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    UUID tx_id3 = UUID.randomUUID();
    String doc1 =
        """
        {
          "_id": "doc1",
          "username": "user1",
          "status" : "active"
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

    String doc1Updated =
        """
        {
          "_id": "doc1",
          "username": "user1",
          "status" : "active",
          "name" : "test"
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
    ValidatingStargateBridge.QueryAssert selectQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("status " + new DocValueHasher().getHash("active").hash()))
            .withPageSize(3)
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id1),
                        Values.of(doc1)),
                    List.of(
                        Values.of(
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc2"))),
                        Values.of(tx_id3),
                        Values.of(doc2))));

    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND key = ? LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    ValidatingStargateBridge.QueryAssert reReadFirstQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("status " + new DocValueHasher().getHash("active").hash()),
                Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
            .withPageSize(3)
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id2),
                        Values.of(doc1))));

    String collectionUpdateCql = UPDATE.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert failedUpdateFirstQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id1))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            // `false` in response for LWT indicate failure
            .returning(List.of(List.of(Values.of(false))));

    ValidatingStargateBridge.QueryAssert failedUpdateRetryFirstQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id2))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            // `false` in response for LWT indicate failure
            .returning(List.of(List.of(Values.of(false))));

    jsonNode = objectMapper.readTree(doc2Updated);
    shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert updateSecondQueryAssert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id3))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            .returning(List.of(List.of(Values.of(true))));

    DBFilterBase.TextFilter filter =
        new DBFilterBase.TextFilter("status", DBFilterBase.MapFilterBase.Operator.EQ, "active");
    FindOperation findOperation =
        new FindOperation(
            COMMAND_CONTEXT,
            List.of(filter),
            DocumentProjector.identityProjector(),
            null,
            3,
            3,
            ReadType.DOCUMENT,
            objectMapper,
            null,
            0,
            0,
            false);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            2,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    selectQueryAssert.assertExecuteCount().isOne();
    reReadFirstQueryAssert.assertExecuteCount().isEqualTo(3);
    failedUpdateFirstQueryAssert.assertExecuteCount().isOne();
    failedUpdateRetryFirstQueryAssert.assertExecuteCount().isEqualTo(3);
    updateSecondQueryAssert.assertExecuteCount().isOne();

    // then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 2)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
    assertThat(result.errors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.fields()).containsEntry("errorCode", "CONCURRENCY_FAILURE");
              assertThat(error.message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
            });
  }

  @Test
  public void findOneAndUpdateWithRetryMultipleFailure() throws Exception {
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    UUID tx_id3 = UUID.randomUUID();
    UUID tx_id4 = UUID.randomUUID();
    String doc1 =
        """
              {
                "_id": "doc1",
                "username": "user1",
                "status" : "active"
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

    String doc1Updated =
        """
              {
                "_id": "doc1",
                "username": "user1",
                "status" : "active",
                "name" : "test"
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
    ValidatingStargateBridge.QueryAssert initialSelectQueryAssert =
        withQuery(
                collectionReadCql,
                Values.of("status " + new DocValueHasher().getHash("active").hash()))
            .withPageSize(3)
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id1),
                        Values.of(doc1)),
                    List.of(
                        Values.of(
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc2"))),
                        Values.of(tx_id3),
                        Values.of(doc2))));

    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? AND key = ? LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    ValidatingStargateBridge.QueryAssert retrySelectQueryDoc1Assert =
        withQuery(
                collectionReadCql,
                Values.of("status " + new DocValueHasher().getHash("active").hash()),
                Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
            .withPageSize(3)
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc1"))),
                        Values.of(tx_id2),
                        Values.of(doc1))));

    ValidatingStargateBridge.QueryAssert retrySelectQueryDoc2Assert =
        withQuery(
                collectionReadCql,
                Values.of("status " + new DocValueHasher().getHash("active").hash()),
                Values.of(CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))))
            .withPageSize(3)
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
                            CustomValueSerializers.getDocumentIdValue(
                                DocumentId.fromString("doc2"))),
                        Values.of(tx_id4),
                        Values.of(doc2))));

    String collectionUpdateCql = UPDATE.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert updateQueryDoc1Assert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id1))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            // `false` in response for LWT indicate failure
            .returning(List.of(List.of(Values.of(false))));

    ValidatingStargateBridge.QueryAssert updateRetryQueryDoc1Assert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id2))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            // `false` in response for LWT indicate failure
            .returning(List.of(List.of(Values.of(false))));

    jsonNode = objectMapper.readTree(doc2Updated);
    shredDocument = shredder.shred(jsonNode);

    ValidatingStargateBridge.QueryAssert updateQueryDoc2Assert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id3))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            .returning(List.of(List.of(Values.of(false))));

    ValidatingStargateBridge.QueryAssert updateRetryQueryDoc2Assert =
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
                Values.of(
                    CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                Values.of(
                    CustomValueSerializers.getTimestampMapValues(
                        shredDocument.queryTimestampValues())),
                Values.of(shredDocument.docJson()),
                Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                Values.of(tx_id4))
            .withColumnSpec(
                List.of(
                    QueryOuterClass.ColumnSpec.newBuilder()
                        .setName("applied")
                        .setType(TypeSpecs.BOOLEAN)
                        .build()))
            .withSerialConsistency(queriesConfig.serialConsistency())
            .returning(List.of(List.of(Values.of(false))));

    DBFilterBase.TextFilter filter =
        new DBFilterBase.TextFilter("status", DBFilterBase.MapFilterBase.Operator.EQ, "active");
    FindOperation findOperation =
        new FindOperation(
            COMMAND_CONTEXT,
            List.of(filter),
            DocumentProjector.identityProjector(),
            null,
            3,
            3,
            ReadType.DOCUMENT,
            objectMapper,
            null,
            0,
            0,
            false);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            2,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
    final CommandResult commandResultSupplier = execute.get();

    initialSelectQueryAssert.assertExecuteCount().isOne();

    retrySelectQueryDoc1Assert.assertExecuteCount().isEqualTo(3);
    updateQueryDoc1Assert.assertExecuteCount().isOne();
    updateRetryQueryDoc1Assert.assertExecuteCount().isEqualTo(3);

    retrySelectQueryDoc2Assert.assertExecuteCount().isEqualTo(3);
    updateQueryDoc2Assert.assertExecuteCount().isOne();
    updateRetryQueryDoc2Assert.assertExecuteCount().isEqualTo(3);

    assertThat(commandResultSupplier)
        .satisfies(
            commandResult -> {
              assertThat(commandResultSupplier.status()).isNotNull();
              assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                  .isEqualTo(2);
              assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                  .isEqualTo(0);
              assertThat(commandResultSupplier.errors()).isNotNull();
              assertThat(commandResultSupplier.errors()).hasSize(1);
              assertThat(commandResultSupplier.errors().get(0).fields().get("errorCode"))
                  .isEqualTo("CONCURRENCY_FAILURE");
              assertThat(commandResultSupplier.errors().get(0).message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1', 'doc2']: Unable to complete transaction due to concurrent transactions");
            });
  }
}
