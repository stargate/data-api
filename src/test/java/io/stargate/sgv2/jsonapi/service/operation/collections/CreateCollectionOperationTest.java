package io.stargate.sgv2.jsonapi.service.operation.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.type.DefaultTupleType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CreateCollectionOperationTest extends OperationTestBase {

  private TestConstants testConstants = new TestConstants();

  @Inject DatabaseLimitsConfig databaseLimitsConfig;

  @Inject ObjectMapper objectMapper;

  @Inject RerankingProvidersConfig rerankingProvidersConfig;

  @Nested
  class Execute {

    private final ColumnDefinitions RESULT_COLUMNS =
        buildColumnDefs(OperationTestBase.TestColumn.ofBoolean("[applied]"));

    private AsyncResultSet mockSuccessSchemaResultset() {
      List<Row> resultRows =
          Arrays.asList(new MockRow(RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      return new MockAsyncResultSet(RESULT_COLUMNS, resultRows, null);
    }

    private AtomicInteger addSchemaChangeCounter(QueryExecutor queryExecutor) {
      var counter = new AtomicInteger();

      when(queryExecutor.executeCreateSchemaChange(eq(requestContext), any()))
          .then(
              invocation -> {
                counter.incrementAndGet();
                return Uni.createFrom().item(mockSuccessSchemaResultset());
              });
      return counter;
    }

    private void addKeyspaceSchema(QueryExecutor queryExecutor) {

      var driverMetadata = mock(Metadata.class);
      when(queryExecutor.getDriverMetadata(any()))
          .thenReturn(Uni.createFrom().item(driverMetadata));

      var allKeyspaces = new HashMap<CqlIdentifier, KeyspaceMetadata>();
      var keyspaceMetadata =
          new DefaultKeyspaceMetadata(
              CqlIdentifier.fromInternal(KEYSPACE_NAME),
              false,
              false,
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>(),
              new HashMap<>());
      allKeyspaces.put(keyspaceMetadata.getName(), keyspaceMetadata);
      when(driverMetadata.getKeyspaces()).thenReturn(allKeyspaces);
    }

    private final CollectionLexicalConfig LEXICAL_CONFIG =
        CollectionLexicalConfig.configForDefault();

    private final CollectionRerankDef RERANKING_DEF = CollectionRerankDef.configForDefault();

    @BeforeEach
    public void init() {}

    @Test
    public void createCollectionNoVector() {
      var queryExecutor = mock(QueryExecutor.class);
      var schemaChangeCounter = addSchemaChangeCounter(queryExecutor);
      addKeyspaceSchema(queryExecutor);

      // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
      // :(
      var operation =
          CreateCollectionOperation.withoutVectorSearch(
              KEYSPACE_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              mock(CQLSessionCache.class),
              COLLECTION_NAME,
              "",
              10,
              false,
              false,
              LEXICAL_CONFIG,
              RERANKING_DEF);

      operation
          .execute(requestContext, queryExecutor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      // 1 create Table + 8 super shredder indexes + lexical index
      assertThat(schemaChangeCounter.get()).isEqualTo(10);
    }

    @Test
    public void createCollectionVector() {
      var queryExecutor = mock(QueryExecutor.class);
      var schemaChangeCounter = addSchemaChangeCounter(queryExecutor);
      addKeyspaceSchema(queryExecutor);

      // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
      // :(
      var operation =
          CreateCollectionOperation.withVectorSearch(
              KEYSPACE_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              mock(CQLSessionCache.class),
              COLLECTION_NAME,
              5,
              "cosine",
              "",
              "",
              10,
              false,
              false,
              LEXICAL_CONFIG,
              RERANKING_DEF);

      operation
          .execute(requestContext, queryExecutor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      // 1 create Table + 8 super shredder indexes + 1 vector index + 1 lexical
      assertThat(schemaChangeCounter.get()).isEqualTo(11);
    }

    @Test
    public void denyAllCollectionNoVector() {
      var queryExecutor = mock(QueryExecutor.class);
      var schemaChangeCounter = addSchemaChangeCounter(queryExecutor);
      addKeyspaceSchema(queryExecutor);

      // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
      // :(
      var operation =
          CreateCollectionOperation.withoutVectorSearch(
              KEYSPACE_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              mock(CQLSessionCache.class),
              COLLECTION_NAME,
              "",
              10,
              false,
              true,
              LEXICAL_CONFIG,
              RERANKING_DEF);

      operation
          .execute(requestContext, queryExecutor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      // 1 create Table + 1 lexical index
      assertThat(schemaChangeCounter.get()).isEqualTo(2);
    }

    @Test
    public void denyAllCollectionVector() {

      var queryExecutor = mock(QueryExecutor.class);
      var schemaChangeCounter = addSchemaChangeCounter(queryExecutor);
      addKeyspaceSchema(queryExecutor);

      // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
      // :(
      var operation =
          CreateCollectionOperation.withVectorSearch(
              KEYSPACE_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              mock(CQLSessionCache.class),
              COLLECTION_NAME,
              5,
              "cosine",
              "",
              "",
              10,
              false,
              true,
              LEXICAL_CONFIG,
              RERANKING_DEF);

      operation
          .execute(requestContext, queryExecutor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();

      // 1 create Table + 1 vector index + 1 lexical
      assertThat(schemaChangeCounter.get()).isEqualTo(3);
    }

    @Test
    public void indexAlreadyDropTable() {
      var queryExecutor = mock(QueryExecutor.class);
      var successResultSet = mockSuccessSchemaResultset();
      addKeyspaceSchema(queryExecutor);

      final AtomicInteger schemaChangeCounter = new AtomicInteger();
      final AtomicInteger schemaDropCounter = new AtomicInteger();

      when(queryExecutor.executeCreateSchemaChange(
              eq(requestContext),
              argThat(
                  simpleStatement ->
                      simpleStatement.getQuery().startsWith("CREATE TABLE IF NOT EXISTS"))))
          .then(
              invocation -> {
                schemaChangeCounter.incrementAndGet();
                return Uni.createFrom().item(successResultSet);
              });

      when(queryExecutor.executeCreateSchemaChange(
              eq(requestContext),
              argThat(
                  simpleStatement -> simpleStatement.getQuery().startsWith("CREATE CUSTOM INDEX"))))
          .then(
              invocation -> {
                schemaChangeCounter.incrementAndGet();
                throw new InvalidQueryException(mock(Node.class), "Index xxxxx already exists");
              });

      when(queryExecutor.executeDropSchemaChange(
              eq(requestContext),
              argThat(
                  simpleStatement ->
                      simpleStatement.getQuery().startsWith("DROP TABLE IF EXISTS"))))
          .then(
              invocation -> {
                schemaDropCounter.incrementAndGet();
                return Uni.createFrom().item(successResultSet);
              });

      // aaron - 19-nov-2025 - best I can tell the sessionCache is not used but we need to pass it
      // :(
      var operation =
          CreateCollectionOperation.withoutVectorSearch(
              KEYSPACE_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              mock(CQLSessionCache.class),
              COLLECTION_NAME,
              "",
              10,
              true,
              false,
              LEXICAL_CONFIG,
              RERANKING_DEF);

      operation
          .execute(requestContext, queryExecutor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem();
      // 1 create Table + 1 index failure
      assertThat(schemaChangeCounter.get()).isEqualTo(2);
      // 1 drop table
      assertThat(schemaDropCounter.get()).isEqualTo(1);
    }

    private List<ColumnMetadata> createCorrectPartitionColumn() {
      List<DataType> tuple =
          Arrays.asList(
              new PrimitiveType(ProtocolConstants.DataType.TINYINT),
              new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
      List<ColumnMetadata> partitionKey = new ArrayList<>();
      partitionKey.add(
          new DefaultColumnMetadata(
              CqlIdentifier.fromInternal("keyspace"),
              CqlIdentifier.fromInternal("collection"),
              CqlIdentifier.fromInternal("key"),
              new DefaultTupleType(tuple),
              false));
      return partitionKey;
    }
  }
}
