package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.*;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.smallrye.faulttolerance.core.util.CompletionStages;
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
    throw new UnsupportedOperationException();
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
    List<Object> boundValues = simpleStatement.getPositionalValues();
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
    return CompletionStages.completedStage(null);
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
