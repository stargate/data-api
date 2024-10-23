package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.type.DataType;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs;
import io.stargate.sgv2.jsonapi.service.schema.tables.ComplexApiDataType;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An attempt to execute a schema change */
public abstract class SchemaAttempt<SchemaT extends SchemaObject>
    extends OperationAttempt<SchemaAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaAttempt.class);

  protected SchemaAttempt(int position, SchemaT schemaObject, RetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
  }

  @Override
  protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {
    var statement = buildStatement();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "execute() - {}, cql={}, values={}",
          positionAndAttemptId(),
          statement.getQuery(),
          statement.getPositionalValues());
    }
    return queryExecutor.executeCreateSchema(statement);
  }

  protected abstract SimpleStatement buildStatement();

  // Add customProperties which has table properties for vectorize
  // Convert value to hex string using the ByteUtils.toHexString
  // This needs to use `createTable.withExtensions()` method in driver when PR
  // (https://github.com/apache/cassandra-java-driver/pull/1964) is released
  protected Map<String, String> encodeAsHexValue(Map<String, String> customProperties) {
    return customProperties.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, e -> ByteUtils.toHexString(e.getValue().getBytes())));
  }

  protected DataType getCqlDataType(ApiDataType apiDataType) {
    if (apiDataType instanceof ComplexApiDataType) {
      return ((ComplexApiDataType) apiDataType).getCqlType();
    } else {
      return ApiDataTypeDefs.from(apiDataType)
          .orElseThrow(() -> new IllegalStateException("Unknown data type: " + apiDataType))
          .getCqlType();
    }
  }

  public static class SchemaRetryPolicy extends RetryPolicy {

    public SchemaRetryPolicy(int maxRetries, Duration delay) {
      super(maxRetries, delay);
    }

    @Override
    public boolean shouldRetry(Throwable throwable) {
      // AARON - this is copied from QueryExecutor.executeSchemaChange()
      return throwable instanceof DriverTimeoutException
          || (throwable instanceof QueryExecutionException
              && !(throwable instanceof InvalidQueryException))
          || (throwable instanceof TruncateException
              && "Failed to interrupt compactions".equals(throwable.getMessage()));
    }
  }
}
