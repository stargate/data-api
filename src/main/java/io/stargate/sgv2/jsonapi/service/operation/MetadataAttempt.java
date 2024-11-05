package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.EmptyAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** An attempt to execute commands that need data from metadata */
public abstract class MetadataAttempt<SchemaT extends SchemaObject>
    extends OperationAttempt<MetadataAttempt<SchemaT>, SchemaT> {
  // this will be set on executeStatement
  // TODO: BETTER CONTROL ON WHEN THIS IS SET AND NOT SET
  protected Optional<KeyspaceMetadata> keyspaceMetadata;

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Create a new {@link OperationAttempt} with the provided position, schema object and retry
   * policy.
   *
   * @param position The 0 based position of the attempt in the container of attempts. Attempts are
   *     ordered by position, for sequential processing and for rebuilding the response in the
   *     correct order (e.g. for inserting many documents)
   * @param schemaObject The schema object that the operation is working with.
   * @param retryPolicy The {@link RetryPolicy} to use when running the operation, if there is no
   *     retry policy then use {@link RetryPolicy#NO_RETRY}
   */
  protected MetadataAttempt(int position, SchemaT schemaObject, RetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
  }

  protected abstract List<String> getNames();

  protected abstract Object getSchema();

  @Override
  protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {

    this.keyspaceMetadata = queryExecutor.getKeyspaceMetadata(schemaObject.name().keyspace());

    // TODO: BETTER ERROR
    if (keyspaceMetadata.isEmpty()) {
      return Uni.createFrom()
          .failure(
              SchemaException.Code.INVALID_KEYSPACE.get(
                  Map.of("keyspace", schemaObject.name().keyspace())));
    }
    return Uni.createFrom().item(new EmptyAsyncResultSet());
  }
}
