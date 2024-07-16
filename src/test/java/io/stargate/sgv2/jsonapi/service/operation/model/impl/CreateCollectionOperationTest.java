package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
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
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CreateCollectionOperationTest extends OperationTestBase {

  private CommandContext<CollectionSchemaObject> COMMAND_CONTEXT =
      new CommandContext<>(COLLECTION_SCHEMA_OBJECT, null, "CreateCollectionCommand", null);

  @Inject DatabaseLimitsConfig databaseLimitsConfig;

  @Inject ObjectMapper objectMapper;

  @Nested
  class Execute {

    private final ColumnDefinitions RESULT_COLUMNS =
        buildColumnDefs(OperationTestBase.TestColumn.ofBoolean("[applied]"));

    @Test
    public void createCollectionNoVector() {
      List<Row> resultRows =
          Arrays.asList(new MockRow(RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet results = new MockAsyncResultSet(RESULT_COLUMNS, resultRows, null);
      final AtomicInteger schemaCounter = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCreateSchemaChange(eq(dataApiRequestInfo), any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession(dataApiRequestInfo)).thenReturn(session);
      Metadata metadata = mock(Metadata.class);
      when(session.getMetadata()).thenReturn(metadata);
      Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces = new HashMap<>();
      DefaultKeyspaceMetadata keyspaceMetadata =
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
      allKeyspaces.put(CqlIdentifier.fromInternal(KEYSPACE_NAME), keyspaceMetadata);
      when(metadata.getKeyspaces()).thenReturn(allKeyspaces);

      CreateCollectionOperation operation =
          CreateCollectionOperation.withoutVectorSearch(
              COMMAND_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              sessionCache,
              COLLECTION_NAME,
              "",
              10,
              false,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      // 1 create Table + 8 super shredder indexes
      assertThat(schemaCounter.get()).isEqualTo(9);
    }

    @Test
    public void createCollectionVector() {
      List<Row> resultRows =
          Arrays.asList(new MockRow(RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet results = new MockAsyncResultSet(RESULT_COLUMNS, resultRows, null);
      final AtomicInteger schemaCounter = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCreateSchemaChange(eq(dataApiRequestInfo), any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession(dataApiRequestInfo)).thenReturn(session);
      Metadata metadata = mock(Metadata.class);
      when(session.getMetadata()).thenReturn(metadata);
      Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces = new HashMap<>();
      DefaultKeyspaceMetadata keyspaceMetadata =
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
      allKeyspaces.put(CqlIdentifier.fromInternal(KEYSPACE_NAME), keyspaceMetadata);
      when(metadata.getKeyspaces()).thenReturn(allKeyspaces);

      CreateCollectionOperation operation =
          CreateCollectionOperation.withVectorSearch(
              COMMAND_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              sessionCache,
              COLLECTION_NAME,
              5,
              "cosine",
              "",
              10,
              false,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      // 1 create Table + 8 super shredder indexes + 1 vector index
      assertThat(schemaCounter.get()).isEqualTo(10);
    }

    @Test
    public void denyAllCollectionNoVector() {
      List<Row> resultRows =
          Arrays.asList(new MockRow(RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet results = new MockAsyncResultSet(RESULT_COLUMNS, resultRows, null);
      final AtomicInteger schemaCounter = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCreateSchemaChange(eq(dataApiRequestInfo), any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession(dataApiRequestInfo)).thenReturn(session);
      Metadata metadata = mock(Metadata.class);
      when(session.getMetadata()).thenReturn(metadata);
      Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces = new HashMap<>();
      DefaultKeyspaceMetadata keyspaceMetadata =
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
      allKeyspaces.put(CqlIdentifier.fromInternal(KEYSPACE_NAME), keyspaceMetadata);
      when(metadata.getKeyspaces()).thenReturn(allKeyspaces);

      CreateCollectionOperation operation =
          CreateCollectionOperation.withoutVectorSearch(
              COMMAND_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              sessionCache,
              COLLECTION_NAME,
              "",
              10,
              false,
              true);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      // 1 create Table
      assertThat(schemaCounter.get()).isEqualTo(1);
    }

    @Test
    public void denyAllCollectionVector() {
      List<Row> resultRows =
          Arrays.asList(new MockRow(RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet results = new MockAsyncResultSet(RESULT_COLUMNS, resultRows, null);
      final AtomicInteger schemaCounter = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCreateSchemaChange(eq(dataApiRequestInfo), any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession(dataApiRequestInfo)).thenReturn(session);
      Metadata metadata = mock(Metadata.class);
      when(session.getMetadata()).thenReturn(metadata);
      Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces = new HashMap<>();
      DefaultKeyspaceMetadata keyspaceMetadata =
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
      allKeyspaces.put(CqlIdentifier.fromInternal(KEYSPACE_NAME), keyspaceMetadata);
      when(metadata.getKeyspaces()).thenReturn(allKeyspaces);

      CreateCollectionOperation operation =
          CreateCollectionOperation.withVectorSearch(
              COMMAND_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              sessionCache,
              COLLECTION_NAME,
              5,
              "cosine",
              "",
              10,
              false,
              true);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      // 1 create Table + 1 vector index
      assertThat(schemaCounter.get()).isEqualTo(2);
    }

    @Test
    public void indexAlreadyDropTable() {
      List<Row> resultRows =
          Arrays.asList(new MockRow(RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet results = new MockAsyncResultSet(RESULT_COLUMNS, resultRows, null);
      final AtomicInteger schemaCounter = new AtomicInteger();
      final AtomicInteger dropCounter = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCreateSchemaChange(
              eq(dataApiRequestInfo),
              argThat(
                  simpleStatement ->
                      simpleStatement.getQuery().startsWith("CREATE TABLE IF NOT EXISTS"))))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      when(queryExecutor.executeCreateSchemaChange(
              eq(dataApiRequestInfo),
              argThat(
                  simpleStatement -> simpleStatement.getQuery().startsWith("CREATE CUSTOM INDEX"))))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                throw new InvalidQueryException(mock(Node.class), "Index xxxxx already exists");
              });

      when(queryExecutor.executeDropSchemaChange(
              eq(dataApiRequestInfo),
              argThat(
                  simpleStatement ->
                      simpleStatement.getQuery().startsWith("DROP TABLE IF EXISTS"))))
          .then(
              invocation -> {
                dropCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });
      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession(dataApiRequestInfo)).thenReturn(session);
      Metadata metadata = mock(Metadata.class);
      when(session.getMetadata()).thenReturn(metadata);
      Map<CqlIdentifier, KeyspaceMetadata> allKeyspaces = new HashMap<>();
      DefaultKeyspaceMetadata keyspaceMetadata =
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
      allKeyspaces.put(CqlIdentifier.fromInternal(KEYSPACE_NAME), keyspaceMetadata);
      when(metadata.getKeyspaces()).thenReturn(allKeyspaces);

      CreateCollectionOperation operation =
          CreateCollectionOperation.withoutVectorSearch(
              COMMAND_CONTEXT,
              databaseLimitsConfig,
              objectMapper,
              sessionCache,
              COLLECTION_NAME,
              "",
              10,
              true,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      // 1 create Table + 1 index failure
      assertThat(schemaCounter.get()).isEqualTo(2);
      // 1 drop table
      assertThat(dropCounter.get()).isEqualTo(1);
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
