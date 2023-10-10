// package io.stargate.sgv2.jsonapi.service.operation.model.impl;
//
// import static org.assertj.core.api.Assertions.assertThat;
//
// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.google.common.collect.ImmutableMap;
// import io.quarkus.test.junit.QuarkusTest;
// import io.quarkus.test.junit.QuarkusTestProfile;
// import io.quarkus.test.junit.TestProfile;
// import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
// import io.stargate.bridge.grpc.TypeSpecs;
// import io.stargate.bridge.grpc.Values;
// import io.stargate.bridge.proto.QueryOuterClass;
// import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
// import io.stargate.sgv2.common.bridge.ValidatingStargateBridge;
// import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
// import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
// import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
// import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
// import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
// import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
// import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
// import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
// import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
// import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
// import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
//// import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
// import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
// import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
// import jakarta.inject.Inject;
// import java.util.List;
// import java.util.Map;
// import java.util.UUID;
// import java.util.function.Supplier;
// import org.apache.commons.lang3.RandomStringUtils;
// import org.junit.jupiter.api.Nested;
// import org.junit.jupiter.api.Test;
//
// @QuarkusTest
// @TestProfile(SerialConsistencyOverrideOperationTest.SerialConsistencyOverrideProfile.class)
// public class SerialConsistencyOverrideOperationTest extends AbstractValidatingStargateBridgeTest
// {
//  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
//  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
//  private static final CommandContext COMMAND_CONTEXT =
//      new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);
//  @Inject QueryExecutor queryExecutor;
//  @Inject ObjectMapper objectMapper;
//  @Inject Shredder shredder;
//
//  public static class SerialConsistencyOverrideProfile implements QuarkusTestProfile {
//    @Override
//    public boolean disableGlobalTestResources() {
//      return true;
//    }
//
//    @Override
//    public Map<String, String> getConfigOverrides() {
//      return ImmutableMap.<String, String>builder()
//          .put("stargate.queries.serial-consistency", "LOCAL_SERIAL")
//          .build();
//    }
//  }
//
//  @Nested
//  class Delete {
//    @Test
//    public void delete() {
//      UUID tx_id = UUID.randomUUID();
//
//      String collectionReadCql =
//          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
//              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
//      ValidatingStargateBridge.QueryAssert readAssert =
//          withQuery(
//                  collectionReadCql,
//                  Values.of(
//                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
//              .withPageSize(1)
//              .withColumnSpec(
//                  List.of(
//                      QueryOuterClass.ColumnSpec.newBuilder()
//                          .setName("key")
//                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
//                          .build(),
//                      QueryOuterClass.ColumnSpec.newBuilder()
//                          .setName("tx_id")
//                          .setType(TypeSpecs.UUID)
//                          .build()))
//              .returning(
//                  List.of(
//                      List.of(
//                          Values.of(
//                              CustomValueSerializers.getDocumentIdValue(
//                                  DocumentId.fromString("doc1"))),
//                          Values.of(tx_id))));
//
//      String collectionDeleteCql =
//          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
//              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
//      ValidatingStargateBridge.QueryAssert deleteAssert =
//          withQuery(
//                  collectionDeleteCql,
//                  Values.of(
//                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
//                  Values.of(tx_id))
//              .withSerialConsistency(QueryOuterClass.Consistency.LOCAL_SERIAL)
//              .returning(List.of(List.of(Values.of(true))));
//
//      LogicalExpression implicitAnd = LogicalExpression.and();
//      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
//      List<DBFilterBase> filters1 =
//          List.of(
//              new DBFilterBase.IDFilter(
//                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
//      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters1);
//
//      FindOperation findOperation =
//          FindOperation.unsortedSingle(
//              COMMAND_CONTEXT,
//              implicitAnd,
//              DocumentProjector.identityProjector(),
//              ReadType.KEY,
//              objectMapper);
//      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);
//      Supplier<CommandResult> execute =
//          operation
//              .execute(queryExecutor)
//              .subscribe()
//              .withSubscriber(UniAssertSubscriber.create())
//              .awaitItem()
//              .getItem();
//
//      // assert query execution
//      readAssert.assertExecuteCount().isOne();
//      deleteAssert.assertExecuteCount().isOne();
//
//      // then result
//      CommandResult result = execute.get();
//      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
//    }
//  }
//
//  @Nested
//  class Insert {
//    static final String INSERT_CQL =
//        "INSERT INTO \"%s\".\"%s\""
//            + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values,
// query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
//            + " VALUES"
//            + " (?, now(), ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";
//
//    @Test
//    public void insert() throws Exception {
//      String document =
//          """
//              {
//                "_id": "doc1",
//                "text": "user1",
//                "number" : 10,
//                "boolean": true,
//                "nullval" : null,
//                "array" : ["a", "b"],
//                "sub_doc" : {"col": "val"}
//              }
//              """;
//
//      JsonNode jsonNode = objectMapper.readTree(document);
//      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
//
//      String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
//      ValidatingStargateBridge.QueryAssert insertAssert =
//          withQuery(
//                  insertCql,
//                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
//                  Values.of(shredDocument.docJson()),
//                  Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
//
// Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
//                  Values.of(
//                      CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
//                  Values.of(
//
// CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
//                  Values.of(
//
// CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
//                  Values.of(
//                      CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
//                  Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
//                  Values.of(
//                      CustomValueSerializers.getTimestampMapValues(
//                          shredDocument.queryTimestampValues())))
//              .withColumnSpec(
//                  List.of(
//                      QueryOuterClass.ColumnSpec.newBuilder()
//                          .setName("applied")
//                          .setType(TypeSpecs.BOOLEAN)
//                          .build()))
//              .withSerialConsistency(QueryOuterClass.Consistency.LOCAL_SERIAL)
//              .returning(List.of(List.of(Values.of(true))));
//
//      InsertOperation operation = new InsertOperation(COMMAND_CONTEXT, shredDocument);
//      Supplier<CommandResult> execute =
//          operation
//              .execute(queryExecutor)
//              .subscribe()
//              .withSubscriber(UniAssertSubscriber.create())
//              .awaitItem()
//              .getItem();
//
//      // assert query execution
//      insertAssert.assertExecuteCount().isOne();
//
//      // then result
//      CommandResult result = execute.get();
//      assertThat(result.status())
//          .hasSize(1)
//          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
//      assertThat(result.errors()).isNull();
//    }
//  }
//
//  @Nested
//  class ReadAndUpdate {
//
//    @Test
//    public void readAndUpdate() throws Exception {
//      String collectionReadCql =
//          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
//              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
//
//      UUID tx_id = UUID.randomUUID();
//      String doc1 =
//          """
//                      {
//                        "_id": "doc1",
//                        "username": "user1"
//                      }
//                      """;
//
//      String doc1Updated =
//          """
//                      {
//                        "_id": "doc1",
//                        "username": "user1",
//                        "name" : "test"
//                      }
//                      """;
//      ValidatingStargateBridge.QueryAssert selectQueryAssert =
//          withQuery(
//                  collectionReadCql,
//                  Values.of(
//                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
//              .withPageSize(1)
//              .withColumnSpec(
//                  List.of(
//                      QueryOuterClass.ColumnSpec.newBuilder()
//                          .setName("key")
//                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
//                          .build(),
//                      QueryOuterClass.ColumnSpec.newBuilder()
//                          .setName("tx_id")
//                          .setType(TypeSpecs.UUID)
//                          .build(),
//                      QueryOuterClass.ColumnSpec.newBuilder()
//                          .setName("doc_json")
//                          .setType(TypeSpecs.VARCHAR)
//                          .build()))
//              .returning(
//                  List.of(
//                      List.of(
//                          Values.of(
//                              CustomValueSerializers.getDocumentIdValue(
//                                  DocumentId.fromString("doc1"))),
//                          Values.of(tx_id),
//                          Values.of(doc1))));
//
//      String update =
//          "UPDATE \"%s\".\"%s\" "
//              + "        SET"
//              + "            tx_id = now(),"
//              + "            exist_keys = ?,"
//              + "            array_size = ?,"
//              + "            array_contains = ?,"
//              + "            query_bool_values = ?,"
//              + "            query_dbl_values = ?,"
//              + "            query_text_values = ?,"
//              + "            query_null_values = ?,"
//              + "            query_timestamp_values = ?,"
//              + "            doc_json  = ?"
//              + "        WHERE "
//              + "            key = ?"
//              + "        IF "
//              + "            tx_id = ?";
//      String collectionUpdateCql = update.formatted(KEYSPACE_NAME, COLLECTION_NAME);
//      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
//      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
//
//      ValidatingStargateBridge.QueryAssert updateQueryAssert =
//          withQuery(
//                  collectionUpdateCql,
//                  Values.of(CustomValueSerializers.getSetValue(shredDocument.existKeys())),
//
// Values.of(CustomValueSerializers.getIntegerMapValues(shredDocument.arraySize())),
//                  Values.of(
//                      CustomValueSerializers.getStringSetValue(shredDocument.arrayContains())),
//                  Values.of(
//
// CustomValueSerializers.getBooleanMapValues(shredDocument.queryBoolValues())),
//                  Values.of(
//
// CustomValueSerializers.getDoubleMapValues(shredDocument.queryNumberValues())),
//                  Values.of(
//                      CustomValueSerializers.getStringMapValues(shredDocument.queryTextValues())),
//                  Values.of(CustomValueSerializers.getSetValue(shredDocument.queryNullValues())),
//                  Values.of(
//                      CustomValueSerializers.getTimestampMapValues(
//                          shredDocument.queryTimestampValues())),
//                  Values.of(shredDocument.docJson()),
//                  Values.of(CustomValueSerializers.getDocumentIdValue(shredDocument.id())),
//                  Values.of(tx_id))
//              .withColumnSpec(
//                  List.of(
//                      QueryOuterClass.ColumnSpec.newBuilder()
//                          .setName("applied")
//                          .setType(TypeSpecs.BOOLEAN)
//                          .build()))
//              .withSerialConsistency(QueryOuterClass.Consistency.LOCAL_SERIAL)
//              .returning(List.of(List.of(Values.of(true))));
//
//      DBFilterBase.IDFilter filter =
//          new DBFilterBase.IDFilter(
//              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
//
//      LogicalExpression implicitAnd = LogicalExpression.and();
//      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
//      List<DBFilterBase> filters1 = List.of(filter);
//      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters1);
//
//      FindOperation findOperation =
//          FindOperation.unsortedSingle(
//              COMMAND_CONTEXT,
//              implicitAnd,
//              DocumentProjector.identityProjector(),
//              ReadType.DOCUMENT,
//              objectMapper);
//      DocumentUpdater documentUpdater =
//          DocumentUpdater.construct(
//              DocumentUpdaterUtils.updateClause(
//                  UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
//      ReadAndUpdateOperation operation =
//          new ReadAndUpdateOperation(
//              COMMAND_CONTEXT,
//              findOperation,
//              documentUpdater,
//              true,
//              false,
//              false,
//              shredder,
//              DocumentProjector.identityProjector(),
//              1,
//              3);
//
//      Supplier<CommandResult> execute =
//          operation
//              .execute(queryExecutor)
//              .subscribe()
//              .withSubscriber(UniAssertSubscriber.create())
//              .awaitItem()
//              .getItem();
//
//      // assert query execution
//      selectQueryAssert.assertExecuteCount().isOne();
//      updateQueryAssert.assertExecuteCount().isOne();
//
//      // then result
//      CommandResult result = execute.get();
//      assertThat(result.status())
//          .hasSize(2)
//          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
//          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
//      assertThat(result.errors()).isNull();
//    }
//  }
// }
