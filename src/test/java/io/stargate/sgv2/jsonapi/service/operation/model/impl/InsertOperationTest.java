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
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class InsertOperationTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final CommandContext COMMAND_CONTEXT =
      new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;
  @Inject QueryExecutor queryExecutor;
  @Inject QueriesConfig queriesConfig;

  @Nested
  class Execute {

    static final String INSERT_CQL =
        "INSERT INTO \"%s\".\"%s\""
            + " (key, tx_id, doc_json, exist_keys, sub_doc_equals, array_size, array_equals, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
            + " VALUES"
            + " (?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";

    @Test
    public void insertOne() throws Exception {
      String document =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"},
            "date_val" : {"$date": 1672531200000 }
          }
          """;

      JsonNode jsonNode = objectMapper.readTree(document);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                  Values.of(shredDocument.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
                  Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      InsertOperation operation = new InsertOperation(COMMAND_CONTEXT, shredDocument);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      insertAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors()).isNull();
    }

    @Test
    public void insertDuplicate() throws Exception {
      String doc1 =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"}
          }
          """;

      final JsonNode jsonNode = objectMapper.readTree(doc1);
      final WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
                  Values.of(shredDocument.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
                  Values.of(CustomValueSerializers.getStringMapValues(shredDocument.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      InsertOperation operation = new InsertOperation(COMMAND_CONTEXT, shredDocument);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      insertAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Failed to insert document with _id 'doc1': Document already exists with the given _id");
                assertThat(error.fields())
                    .containsEntry("exceptionClass", "JsonApiException")
                    .containsEntry("errorCode", "DOCUMENT_ALREADY_EXISTS");
              });
    }

    @Test
    public void insertManyOrdered() throws Exception {
      String document1 =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"}
          }
          """;
      String document2 =
          """
          {
            "_id": "doc2",
            "text": "user2",
            "number" : 11,
            "boolean": false,
            "nullval" : null,
            "array" : ["c", "d"],
            "sub_doc" : {"col": "lav"}
          }
          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertFirstAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument1.id())),
                  Values.of(shredDocument1.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument1.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument1.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument1.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument1.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument1.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));
      ValidatingStargateBridge.QueryAssert insertSecondAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument2.id())),
                  Values.of(shredDocument2.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument2.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument2.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument2.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument2.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument2.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      InsertOperation operation =
          new InsertOperation(COMMAND_CONTEXT, List.of(shredDocument1, shredDocument2), true);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      insertFirstAssert.assertExecuteCount().isOne();
      insertSecondAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(
              CommandStatus.INSERTED_IDS,
              List.of(new DocumentId.StringId("doc1"), new DocumentId.StringId("doc2")));
      assertThat(result.errors()).isNull();
    }

    @Test
    public void insertManyUnordered() throws Exception {
      String document1 =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"}
          }
          """;
      String document2 =
          """
          {
            "_id": "doc2",
            "text": "user2",
            "number" : 11,
            "boolean": false,
            "nullval" : null,
            "array" : ["c", "d"],
            "sub_doc" : {"col": "lav"}
          }
          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertFirstAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument1.id())),
                  Values.of(shredDocument1.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument1.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument1.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument1.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument1.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument1.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));
      ValidatingStargateBridge.QueryAssert insertSecondAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument2.id())),
                  Values.of(shredDocument2.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument2.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument2.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument2.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument2.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument2.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      InsertOperation operation =
          new InsertOperation(COMMAND_CONTEXT, List.of(shredDocument1, shredDocument2), false);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      insertFirstAssert.assertExecuteCount().isOne();
      insertSecondAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1);
      assertThat(result.status().get(CommandStatus.INSERTED_IDS))
          .asList()
          .containsExactlyInAnyOrder(
              new DocumentId.StringId("doc1"), new DocumentId.StringId("doc2"));
      assertThat(result.errors()).isNull();
    }

    // failure modes

    @Test
    public void failureOrdered() throws Exception {
      // unordered first query fail
      String document1 =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"}
          }
          """;
      String document2 =
          """
          {
            "_id": "doc2",
            "text": "user2",
            "number" : 11,
            "boolean": false,
            "nullval" : null,
            "array" : ["c", "d"],
            "sub_doc" : {"col": "lav"}
          }
          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertFirstAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument1.id())),
                  Values.of(shredDocument1.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument1.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument1.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument1.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument1.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument1.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returningFailure(new RuntimeException("Ivan breaks the test."));

      InsertOperation operation =
          new InsertOperation(COMMAND_CONTEXT, List.of(shredDocument1, shredDocument2), true);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      // second query never executed
      insertFirstAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo("Failed to insert document with _id 'doc1': Ivan breaks the test.");
                assertThat(error.fields()).containsEntry("exceptionClass", "RuntimeException");
              });
    }

    @Test
    public void failureOrderedLastFails() throws Exception {
      // unordered first query OK, second fail
      String document1 =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"}
          }
          """;
      String document2 =
          """
          {
            "_id": "doc2",
            "text": "user2",
            "number" : 11,
            "boolean": false,
            "nullval" : null,
            "array" : ["c", "d"],
            "sub_doc" : {"col": "lav"}
          }
          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertFirstAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument1.id())),
                  Values.of(shredDocument1.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument1.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument1.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument1.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument1.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument1.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));
      ValidatingStargateBridge.QueryAssert insertSecondAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument2.id())),
                  Values.of(shredDocument2.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument2.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument2.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument2.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument2.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument2.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returningFailure(new RuntimeException("Ivan really breaks the test."));

      InsertOperation operation =
          new InsertOperation(COMMAND_CONTEXT, List.of(shredDocument1, shredDocument2), true);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      // second query never executed
      insertFirstAssert.assertExecuteCount().isOne();
      insertSecondAssert.assertExecuteCount().isOne();

      // then result contains both insert and error
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Failed to insert document with _id 'doc2': Ivan really breaks the test.");
                assertThat(error.fields()).containsEntry("exceptionClass", "RuntimeException");
              });
    }

    @Test
    public void failureUnorderedPartial() throws Exception {
      // unordered one query fail
      String document1 =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"}
          }
          """;
      String document2 =
          """
          {
            "_id": "doc2",
            "text": "user2",
            "number" : 11,
            "boolean": false,
            "nullval" : null,
            "array" : ["c", "d"],
            "sub_doc" : {"col": "lav"}
          }
          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertFirstAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument1.id())),
                  Values.of(shredDocument1.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument1.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument1.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument1.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument1.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument1.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returningFailure(new RuntimeException("Ivan breaks the test."));
      ValidatingStargateBridge.QueryAssert insertSecondAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument2.id())),
                  Values.of(shredDocument2.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument2.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument2.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument2.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument2.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument2.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      InsertOperation operation =
          new InsertOperation(COMMAND_CONTEXT, List.of(shredDocument1, shredDocument2), false);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      // second query never executed
      insertFirstAssert.assertExecuteCount().isOne();
      insertSecondAssert.assertExecuteCount().isOne();

      // then result has both insert id and errors
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc2")));
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo("Failed to insert document with _id 'doc1': Ivan breaks the test.");
                assertThat(error.fields()).containsEntry("exceptionClass", "RuntimeException");
              });
    }

    @Test
    public void failureUnorderedAll() throws Exception {
      // unordered both queries fail
      String document1 =
          """
          {
            "_id": "doc1",
            "text": "user1",
            "number" : 10,
            "boolean": true,
            "nullval" : null,
            "array" : ["a", "b"],
            "sub_doc" : {"col": "val"}
          }
          """;
      String document2 =
          """
          {
            "_id": "doc2",
            "text": "user2",
            "number" : 11,
            "boolean": false,
            "nullval" : null,
            "array" : ["c", "d"],
            "sub_doc" : {"col": "lav"}
          }
          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert insertFirstAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument1.id())),
                  Values.of(shredDocument1.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument1.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument1.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument1.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument1.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument1.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument1.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument1.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returningFailure(new RuntimeException("Ivan breaks the test."));
      ValidatingStargateBridge.QueryAssert insertSecondAssert =
          withQuery(
                  insertCql,
                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument2.id())),
                  Values.of(shredDocument2.docJson()),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.existKeys())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.subDocEquals())),
                  Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument2.arraySize())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.arrayEquals())),
                  Values.of(
                      CustomValueSerializers.getStringSetValue(shredDocument2.arrayContains())),
                  Values.of(
                      CustomValueSerializers.getBooleanMapValues(shredDocument2.queryBoolValues())),
                  Values.of(
                      CustomValueSerializers.getDoubleMapValues(
                          shredDocument2.queryNumberValues())),
                  Values.of(
                      CustomValueSerializers.getStringMapValues(shredDocument2.queryTextValues())),
                  Values.of(CustomValueSerializers.getSetValue(shredDocument2.queryNullValues())),
                  Values.of(
                      CustomValueSerializers.getTimestampMapValues(
                          shredDocument2.queryTimestampValues())))
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("applied")
                          .setType(TypeSpecs.BOOLEAN)
                          .build()))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returningFailure(new RuntimeException("Ivan really breaks the test."));

      InsertOperation operation =
          new InsertOperation(COMMAND_CONTEXT, List.of(shredDocument1, shredDocument2), false);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      // second query never executed
      insertFirstAssert.assertExecuteCount().isOne();
      insertSecondAssert.assertExecuteCount().isOne();

      // then result has 2 errors
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .hasSize(2)
          .extracting(CommandResult.Error::message)
          .containsExactlyInAnyOrder(
              "Failed to insert document with _id 'doc1': Ivan breaks the test.",
              "Failed to insert document with _id 'doc2': Ivan really breaks the test.");
    }
  }
}
