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
import com.datastax.oss.driver.internal.core.data.DefaultTupleValue;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.smallrye.faulttolerance.core.util.CompletionStages;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.types.CodecRegistry;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.TupleType;
import org.apache.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriterSession implements CqlSession {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriterSession.class);
  private static final AtomicInteger counter = new AtomicInteger(0);
  private final String sessionId;
  private final String keyspace;
  private final String table;
  private final ColumnDefinitions responseColumnDefinitions;
  private final CQLSessionCache cqlSessionCache;
  private final CQLSessionCache.SessionCacheKey cacheKey;
  private final CQLSSTableWriter cqlsSSTableWriter;

  public FileWriterSession(
      CQLSessionCache cqlSessionCache,
      CQLSessionCache.SessionCacheKey cacheKey,
      FileWriterParams fileWriterParams)
      throws IOException {
    this.cqlSessionCache = cqlSessionCache;
    this.cacheKey = cacheKey;
    this.sessionId = "fileWriterSession" + counter.getAndIncrement();
    this.keyspace = fileWriterParams.keyspaceName();
    this.table = fileWriterParams.tableName();
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
    if (Files.exists(Path.of(fileWriterParams.ssTableOutputDirectory()))) {
      recursiveDelete(Path.of(fileWriterParams.ssTableOutputDirectory()));
    }
    Files.createDirectories(Path.of(fileWriterParams.ssTableOutputDirectory()));
    this.cqlsSSTableWriter =
        CQLSSTableWriter.builder()
            .inDirectory(fileWriterParams.ssTableOutputDirectory())
            .forTable(fileWriterParams.createTableCQL())
            .using(fileWriterParams.insertStatementCQL())
            .build();
    DatabaseDescriptor.getRawConfig().data_file_directories =
        new String[] {fileWriterParams.ssTableOutputDirectory()};
    DatabaseDescriptor.getRawConfig().commitlog_directory =
        fileWriterParams.ssTableOutputDirectory() + "/commitlog";
    DatabaseDescriptor.getRawConfig().saved_caches_directory =
        fileWriterParams.ssTableOutputDirectory() + "/saved_caches";
    DatabaseDescriptor.getRawConfig().hints_directory =
        fileWriterParams.ssTableOutputDirectory() + "/hints";
    DatabaseDescriptor.getRawConfig().metadata_directory =
        fileWriterParams.ssTableOutputDirectory() + "/metadata_directory";
    DatabaseDescriptor.getRawConfig().commitlog_sync = Config.CommitLogSync.batch;
  }

  public static void recursiveDelete(Path directory) throws IOException {
    try (Stream<Path> files = Files.walk(directory)) {
      files
          .sorted(Comparator.reverseOrder())
          .forEach(
              file -> {
                try {
                  Files.delete(file);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
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
    List<Object> boundValues =
        new ArrayList<>(
            simpleStatement
                .getPositionalValues()); // TODO-SL handle key in the place where originally
    //                                 // constructed
    DefaultTupleValue tupleValue = (DefaultTupleValue) boundValues.get(0);
    TupleValue cxTupleValue =
        TupleType.of(
                org.apache.cassandra.transport.ProtocolVersion.CURRENT,
                new CodecRegistry(),
                DataType.tinyint(),
                DataType.text())
            .newValue(tupleValue.get(0, TypeCodecs.TINYINT), tupleValue.get(1, TypeCodecs.TEXT));
    boundValues.set(0, cxTupleValue);
    System.out.println("Query: " + query);
    System.out.println("Bound values: " + boundValues);
    List<ByteBuffer> buffers = new ArrayList<>();
    try {
      this.cqlsSSTableWriter.addRow(boundValues);
      buffers.add(TypeCodecs.BOOLEAN.encode(Boolean.TRUE, ProtocolVersion.DEFAULT));
      CompletionStage<AsyncResultSet> resultSetCompletionStage =
          CompletionStages.completedStage(
              new FileWriterAsyncResultSet(
                  responseColumnDefinitions,
                  new FileWriterResponseRow(responseColumnDefinitions, 0, buffers)));
      return (ResultT) resultSetCompletionStage;
    } catch (IOException e) {
      LOGGER.error("Error writing to SSTable", e);
      throw new RuntimeException(e);
    }
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    try {
      this.cqlsSSTableWriter.close();
    } catch (IOException e) {

      throw new RuntimeException(e);
    }
    cqlSessionCache.removeSession(this.cacheKey);
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
