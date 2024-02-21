package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.smallrye.faulttolerance.core.util.CompletionStages;
import io.stargate.sgv2.jsonapi.service.processor.SSTableWriterStatus;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class FileWriterSession implements CqlSession {
  private static final AtomicInteger counter = new AtomicInteger(0);
  private final String sessionId;
  private final String keyspace;
  private final String table;
  private final ColumnDefinitions responseColumnDefinitions;

  public FileWriterSession(String keyspace, String table) {
    this.sessionId = "fileWriterSession" + counter.getAndIncrement();
    this.keyspace = keyspace;
    this.table = table;
    this.responseColumnDefinitions =
        DefaultColumnDefinitions.valueOf(
            List.of(
                new DefaultColumnDefinition(
                    new ColumnSpec(
                        keyspace,
                        table,
                        "[applied]",
                        0,
                        RawType.PRIMITIVES.get(ProtocolConstants.DataType.BOOLEAN)),
                    null)));
  }

  @NonNull
  @Override
  public String getName() {
    return this.sessionId;
  }

  @NonNull
  @Override
  public Metadata getMetadata() {
    return new Metadata() {

      @NonNull
      @Override
      public Map<UUID, Node> getNodes() {
        throw new UnsupportedOperationException();
      }

      @NonNull
      @Override
      public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
        return getKeyspacesMetadata();
      }

      @NonNull
      @Override
      public Optional<TokenMap> getTokenMap() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public Map<CqlIdentifier, KeyspaceMetadata> getKeyspacesMetadata() {
    @NonNull Map<CqlIdentifier, TableMetadata> tables = getTableMetadata();
    return Map.of(
        CqlIdentifier.fromCql(keyspace),
        new DefaultKeyspaceMetadata(
            CqlIdentifier.fromCql(keyspace),
            false,
            false,
            Collections.emptyMap(),
            Collections.emptyMap(),
            tables,
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>()));
  }

  private Map<CqlIdentifier, TableMetadata> getTableMetadata() {
    List<ColumnMetadata> partitionColumn =
        Lists.newArrayList(
            new DefaultColumnMetadata(
                CqlIdentifier.fromInternal(keyspace),
                CqlIdentifier.fromInternal(table),
                CqlIdentifier.fromInternal("key"),
                DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT),
                false));
    Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
    columns.put(
        CqlIdentifier.fromInternal("tx_id"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("tx_id"),
            DataTypes.TIMEUUID,
            false));
    columns.put(
        CqlIdentifier.fromInternal("doc_json"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("doc_json"),
            DataTypes.TEXT,
            false));
    columns.put(
        CqlIdentifier.fromInternal("exist_keys"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("exist_keys"),
            DataTypes.setOf(DataTypes.TEXT),
            false));
    columns.put(
        CqlIdentifier.fromInternal("array_size"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("array_size"),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT),
            false));
    columns.put(
        CqlIdentifier.fromInternal("array_contains"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("array_contains"),
            DataTypes.setOf(DataTypes.TEXT),
            false));
    columns.put(
        CqlIdentifier.fromInternal("query_bool_values"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("query_bool_values"),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.TINYINT),
            false));
    columns.put(
        CqlIdentifier.fromInternal("query_dbl_values"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("query_dbl_values"),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.DECIMAL),
            false));
    columns.put(
        CqlIdentifier.fromInternal("query_text_values"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("query_text_values"),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT),
            false));
    columns.put(
        CqlIdentifier.fromInternal("query_timestamp_values"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("query_timestamp_values"),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.TIMESTAMP),
            false));
    columns.put(
        CqlIdentifier.fromInternal("query_null_values"),
        new DefaultColumnMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            CqlIdentifier.fromInternal("query_null_values"),
            DataTypes.setOf(DataTypes.TEXT),
            false));
    return Map.of(
        CqlIdentifier.fromCql(table),
        new DefaultTableMetadata(
            CqlIdentifier.fromInternal(keyspace),
            CqlIdentifier.fromInternal(table),
            UUID.randomUUID(),
            false,
            false,
            partitionColumn,
            new HashMap<>(),
            columns,
            new HashMap<>(),
            new HashMap<>()));
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(@Nullable Boolean newValue) {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public DriverContext getContext() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public Optional<CqlIdentifier> getKeyspace() {
    return Optional.of(CqlIdentifier.fromCql(this.keyspace));
  }

  @NonNull
  @Override
  public Optional<Metrics> getMetrics() {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      @NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
    SimpleStatement simpleStatement = (SimpleStatement) request;
    String query = simpleStatement.getQuery();
    Map<CqlIdentifier, Object> boundValues = simpleStatement.getNamedValues();
    System.out.println("Query: " + query);
    System.out.println("Bound values: " + boundValues);
    List<ByteBuffer> buffers = new ArrayList<>();
    buffers.add(TypeCodecs.BOOLEAN.encode(Boolean.TRUE, ProtocolVersion.DEFAULT));
    CompletionStage<AsyncResultSet> resultSetCompletionStage =
        CompletionStages.completedStage(
            new FileWriterAsyncResultSet(
                responseColumnDefinitions,
                new FileWriterResponseRow(responseColumnDefinitions, 0, buffers)));
    return (ResultT) resultSetCompletionStage;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    throw new UnsupportedOperationException();
  }

  public String getNamespace() {
    return this.keyspace;
  }

  public String getCollection() {
    return this.table;
  }

  public SSTableWriterStatus getStatus() {
    return new SSTableWriterStatus(this.keyspace, this.table);
  }
}
