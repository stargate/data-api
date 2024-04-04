package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DatabaseLimitsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import jakarta.inject.Inject;
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

  private CommandContext COMMAND_CONTEXT =
      new CommandContext(KEYSPACE_NAME, COLLECTION_NAME, "CreateCollectionCommand", null);

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
      when(queryExecutor.executeCreateSchemaChange(any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession()).thenReturn(session);
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
              .execute(queryExecutor)
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
      when(queryExecutor.executeCreateSchemaChange(any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession()).thenReturn(session);
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
              .execute(queryExecutor)
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
      when(queryExecutor.executeCreateSchemaChange(any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession()).thenReturn(session);
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
              .execute(queryExecutor)
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
      when(queryExecutor.executeCreateSchemaChange(any()))
          .then(
              invocation -> {
                schemaCounter.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      CQLSessionCache sessionCache = mock(CQLSessionCache.class);
      CqlSession session = mock(CqlSession.class);
      when(sessionCache.getSession()).thenReturn(session);
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
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();
      // 1 create Table + 1 vector index
      assertThat(schemaCounter.get()).isEqualTo(2);
    }
  }
}
