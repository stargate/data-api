package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.types.CodecRegistry;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.TupleType;
import org.apache.cassandra.cql3.functions.types.TupleValue;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A session that writes data to SSTable files. This implements the {@link CqlSession} interface so
 * that this can be cached by the {@link CQLSessionCache}. This class primarily takes the CQL insert
 * statement, bound values and writes the data to SSTable files.
 */
public class FileWriterSession implements CqlSession {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriterSession.class);
  private final AtomicInteger insertsSucceeded = new AtomicInteger(0);
  private final AtomicInteger insertsFailed = new AtomicInteger(0);
  private final String sessionId;
  private final String keyspace;
  private final String table;
  private final ColumnDefinitions responseColumnDefinitions;
  private final CQLSessionCache cqlSessionCache;
  private final CQLSessionCache.SessionCacheKey cacheKey;
  private final String ssTableOutputDirectory;
  private final CQLSSTableWriter cqlsSSTableWriter;
  private final int fileWriterBufferSizeInMB;
  private final FileWriterParams fileWriterParams;

  /**
   * Creates a new instance of {@link FileWriterSession}
   *
   * @param cqlSessionCache The cache that holds the session
   * @param cacheKey The cache key
   * @param sessionId The session id
   * @param fileWriterParams The parameters required to write the data to SSTable files
   * @throws IOException If there are IO errors while creating SSTable files related directories
   */
  public FileWriterSession(
      CQLSessionCache cqlSessionCache,
      CQLSessionCache.SessionCacheKey cacheKey,
      String sessionId,
      FileWriterParams fileWriterParams)
      throws IOException {
    this.fileWriterParams = fileWriterParams;
    this.cqlSessionCache = cqlSessionCache;
    this.cacheKey = cacheKey;
    this.sessionId = sessionId;
    this.keyspace = fileWriterParams.keyspaceName();
    this.table = fileWriterParams.tableName();
    this.responseColumnDefinitions = getOfflineWriterResponseRowCols(keyspace, table);
    if (!Files.exists(Path.of(fileWriterParams.ssTableOutputDirectory()))) {
      throw new FileNotFoundException(
          "Directory does not exist: " + fileWriterParams.ssTableOutputDirectory());
    }
    this.ssTableOutputDirectory = fileWriterParams.ssTableOutputDirectory();
    this.fileWriterBufferSizeInMB = fileWriterParams.fileWriterBufferSizeInMB();
    CQLSSTableWriter.Builder cqlSSTableWriterBuilder =
        CQLSSTableWriter.builder()
            .inDirectory(ssTableOutputDirectory)
            .forTable(fileWriterParams.createTableCQL())
            .using(fileWriterParams.insertStatementCQL());
    if (this.fileWriterBufferSizeInMB > 0) {
      cqlSSTableWriterBuilder.withBufferSizeInMB(this.fileWriterBufferSizeInMB);
    }
    this.cqlsSSTableWriter = cqlSSTableWriterBuilder.build();
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Create table CQL: {}", fileWriterParams.createTableCQL());
      LOGGER.trace("Insert statement: {}", fileWriterParams.insertStatementCQL());
    }
    String emptyCassandraDataDirectory = getEmptyCassandraDir();
    if (!new File(emptyCassandraDataDirectory).exists()) {
      if (!new File(emptyCassandraDataDirectory).mkdirs()) {
        throw new RuntimeException("Failed to create directory: " + emptyCassandraDataDirectory);
      }
    }
    DatabaseDescriptor.getRawConfig().data_file_directories =
        new String[] {emptyCassandraDataDirectory + File.separator + "data"};
    DatabaseDescriptor.getRawConfig().commitlog_directory =
        emptyCassandraDataDirectory + File.separator + "commitlog";
    DatabaseDescriptor.getRawConfig().saved_caches_directory =
        emptyCassandraDataDirectory + File.separator + "saved_caches";
    DatabaseDescriptor.getRawConfig().hints_directory =
        emptyCassandraDataDirectory + File.separator + "hints";
    DatabaseDescriptor.getRawConfig().metadata_directory =
        emptyCassandraDataDirectory + File.separator + "metadata_directory";
    DatabaseDescriptor.getRawConfig().commitlog_sync = Config.CommitLogSync.batch;
  }

  private String getEmptyCassandraDir() {
    return System.getProperty("java.io.tmpdir")
        + File.separator
        + "cassandra"
        + File.separator
        + "data";
  }

  private ColumnDefinitions getOfflineWriterResponseRowCols(String keyspace, String table) {
    return DefaultColumnDefinitions.valueOf(
        List.of(
            new DefaultColumnDefinition(
                new ColumnSpec(
                    keyspace,
                    table,
                    "[applied]",
                    0,
                    RawType.PRIMITIVES.get(ProtocolConstants.DataType.BOOLEAN)),
                AttachmentPoint.NONE)));
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
    List<Object> boundValues = new ArrayList<>(simpleStatement.getPositionalValues());
    resetColumnValues(boundValues);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Bound values: {}", boundValues);
    }
    List<ByteBuffer> buffers = new ArrayList<>();
    try {
      this.cqlsSSTableWriter.addRow(boundValues);
      buffers.add(TypeCodecs.BOOLEAN.encode(Boolean.TRUE, ProtocolVersion.DEFAULT));
      CompletionStage<AsyncResultSet> resultSetCompletionStage =
          CompletionStages.completedStage(
              new FileWriterAsyncResultSet(
                  responseColumnDefinitions,
                  new FileWriterResponseRow(responseColumnDefinitions, 0, buffers)));
      insertsSucceeded.incrementAndGet();
      return (ResultT) resultSetCompletionStage;
    } catch (Exception e) {
      LOGGER.error("Error writing to SSTable", e);
      insertsFailed.incrementAndGet();
      throw new RuntimeException(e);
    }
  }

  private void resetColumnValues(List<Object> boundValues) {
    // Change _id from com.datastax.oss.driver.internal.core.data.DefaultTupleValue to
    // org.apache.cassandra.cql3.functions.types.TupleValue
    DefaultTupleValue tupleValue = (DefaultTupleValue) boundValues.get(0);
    TupleValue cxTupleValue =
        TupleType.of(
                org.apache.cassandra.transport.ProtocolVersion.CURRENT,
                new CodecRegistry(),
                DataType.tinyint(),
                DataType.text())
            .newValue(tupleValue.get(0, TypeCodecs.TINYINT), tupleValue.get(1, TypeCodecs.TEXT));
    boundValues.set(0, cxTupleValue);
    if (this.fileWriterParams.vectorEnabled()) {
      // Change $vector from com.datastax.oss.driver.api.core.data.CqlVector to java.nio.ByteBuffer
      int vectorColumnIndex =
          boundValues.size()
              - 1; // TODO-SL: Need to find a better way to identify the vector column index
      CqlVector<Float> cqlVector = (CqlVector<Float>) boundValues.get(vectorColumnIndex);
      ByteBuffer encodedVectorData =
          TypeCodecs.vectorOf(cqlVector.size(), TypeCodecs.FLOAT)
              .encode(cqlVector, ProtocolVersion.DEFAULT);
      boundValues.set(vectorColumnIndex, encodedVectorData);
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

  public OfflineWriterSessionStatus getStatus() {
    return new OfflineWriterSessionStatus(
        this.sessionId,
        this.keyspace,
        this.table,
        this.ssTableOutputDirectory,
        this.fileWriterBufferSizeInMB,
        getDirectorySizeInBytesTopLevel(this.ssTableOutputDirectory),
        insertsSucceeded.get(),
        insertsFailed.get());
  }

  /**
   * Returns the size of the directory in bytes by summing the size of all the files in the
   * directory only at the top level and not recursively by traversing the subdirectories.
   *
   * @param dataDirectory The directory
   * @return The size of the directory in bytes
   */
  public static int getDirectorySizeInBytesTopLevel(String dataDirectory) {
    File file = new File(dataDirectory);
    long size = 0;
    for (File f : Objects.requireNonNull(file.listFiles())) {
      size += f.length();
    }
    return (int) size;
  }
}
