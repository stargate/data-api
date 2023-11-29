package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
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
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.inject.Inject;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ReadAndUpdateOperationTest extends OperationTestBase {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final CommandContext COMMAND_CONTEXT =
      new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  private static final CommandContext COMMAND_VECTOR_CONTEXT =
      new CommandContext(
          KEYSPACE_NAME, COLLECTION_NAME, true, CollectionSettings.SimilarityFunction.COSINE, null);

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;


  private static String UPDATE =
      "UPDATE \"%s\".\"%s\" "
          + "        SET"
          + "            tx_id = now(),"
          + "            exist_keys = ?,"
          + "            array_size = ?,"
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

  private static String UPDATE_VECTOR =
      "UPDATE \"%s\".\"%s\" "
          + "        SET"
          + "            tx_id = now(),"
          + "            exist_keys = ?,"
          + "            array_size = ?,"
          + "            array_contains = ?,"
          + "            query_bool_values = ?,"
          + "            query_dbl_values = ?,"
          + "            query_text_values = ?,"
          + "            query_null_values = ?,"
          + "            query_timestamp_values = ?,"
          + "            query_vector_value = ?,"
          + "            doc_json  = ?"
          + "        WHERE "
          + "            key = ?"
          + "        IF "
          + "            tx_id = ?";

  private final ColumnDefinitions KEY_TXID_JSON_COLUMNS =
          buildColumnDefs(
                  TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"), TestColumn.ofVarchar("doc_json"));
  private MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }

  private MockRow resultRow(int index, String key, UUID txId, String doc) {
    return new MockRow(
            KEY_TXID_JSON_COLUMNS,
            index,
            Arrays.asList(byteBufferForKey(key), byteBufferFrom(txId), byteBufferFrom(doc)));
  }

  private final ColumnDefinitions COLUMNS_APPLIED =
          buildColumnDefs(TestColumn.ofBoolean("[applied]"));

  private SimpleStatement nonVectorUpdateStatement(WritableShreddedDocument shredDocument) {
    String updateCql = UPDATE.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    return SimpleStatement.newInstance(
            updateCql,
            CQLBindValues.getSetValue(shredDocument.existKeys()),
            CQLBindValues.getIntegerMapValues(shredDocument.arraySize()),
            CQLBindValues.getStringSetValue(shredDocument.arrayContains()),
            CQLBindValues.getBooleanMapValues(shredDocument.queryBoolValues()),
            CQLBindValues.getDoubleMapValues(shredDocument.queryNumberValues()),
            CQLBindValues.getStringMapValues(shredDocument.queryTextValues()),
            CQLBindValues.getSetValue(shredDocument.queryNullValues()),
            CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()),
            shredDocument.docJson(),
            CQLBindValues.getDocumentIdValue(shredDocument.id()),
            UUID.randomUUID()
            );
  }

  private SimpleStatement vectorUpdateStatement(WritableShreddedDocument shredDocument) {
    String updateCql = UPDATE_VECTOR.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    return SimpleStatement.newInstance(
            updateCql,
            CQLBindValues.getSetValue(shredDocument.existKeys()),
            CQLBindValues.getIntegerMapValues(shredDocument.arraySize()),
            CQLBindValues.getStringSetValue(shredDocument.arrayContains()),
            CQLBindValues.getBooleanMapValues(shredDocument.queryBoolValues()),
            CQLBindValues.getDoubleMapValues(shredDocument.queryNumberValues()),
            CQLBindValues.getStringMapValues(shredDocument.queryTextValues()),
            CQLBindValues.getSetValue(shredDocument.queryNullValues()),
            CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()),
            CQLBindValues.getVectorValue(shredDocument.queryVectorValues()),
            shredDocument.docJson(),
            CQLBindValues.getDocumentIdValue(shredDocument.id()),
            CQLBindValues.getStringSetValue(shredDocument.arrayContains()),
            UUID.randomUUID()
    );
  }
  @Nested
  class UpdateOne {
    @Test
    public void happyPath() throws Exception {
      UUID tx_id = UUID.randomUUID();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      //read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
            {
              "_id": "doc1",
              "username": "user1"
            }
            """;
      SimpleStatement stmt1 =
              SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", tx_id, doc1));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(stmt1), any(), anyInt()))
              .then(
                      invocation -> {
                        callCount1.incrementAndGet();
                        return Uni.createFrom().item(results1);
                      });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
              List.of(
                      new DBFilterBase.IDFilter(
                              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
              FindOperation.unsortedSingle(
                      COMMAND_VECTOR_CONTEXT,
                      implicitAnd,
                      DocumentProjector.identityProjector(),
                      ReadType.DOCUMENT,
                      objectMapper);

      //update
      String doc1Updated =
          """
            {
              "_id": "doc1",
              "username": "user1",
              "$vector" : [0.11,0.22,0.33,0.44]
            }
            """;

      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      Log.error("shred " + shredDocument);
      SimpleStatement stmt2 = vectorUpdateStatement(shredDocument);
      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(stmt2)))
              .then(
                      invocation -> {
                        callCount2.incrementAndGet();
                        return Uni.createFrom().item(results2);
                      });


      String updateClause =
          """
                   { "$set" : { "$vector" : [0.11,0.22,0.33,0.44] }}
              """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(objectMapper.readValue(updateClause, UpdateClause.class));
      ReadAndUpdateOperation operation =
          new ReadAndUpdateOperation(
              COMMAND_VECTOR_CONTEXT,
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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
//      assertThat(result.status())
//          .hasSize(2)
//          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
//          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
//      assertThat(result.errors()).isNull();
    }
  }
}
