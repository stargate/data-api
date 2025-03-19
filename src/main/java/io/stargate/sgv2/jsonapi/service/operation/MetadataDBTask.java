package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.EmptyAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * A task to read metadata from the database, such as list tables.
 *
 * @param <SchemaT>
 */
public abstract class MetadataDBTask<SchemaT extends SchemaObject> extends DBTask<SchemaT> {

  // Re-use the matcher for a collection, anything not a collection is a table
  protected static final Predicate<TableMetadata> TABLE_MATCHER =
      new CollectionTableMatcher().negate();

  // this will be set on executeStatement
  // TODO: BETTER CONTROL ON WHEN THIS IS SET AND NOT SET
  protected Optional<KeyspaceMetadata> keyspaceMetadata;

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected MetadataDBTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory) {
    super(position, schemaObject, retryPolicy, exceptionHandlerFactory);
  }

  protected abstract List<String> getNames();

  protected abstract Object getSchema();

  @Override
  protected AsyncResultSetSupplier buildDBResultSupplier(
      CommandContext<SchemaT> commandContext, CommandQueryExecutor queryExecutor) {

    this.keyspaceMetadata = queryExecutor.getKeyspaceMetadata(schemaObject.name().keyspace());

    // TODO: BETTER ERROR
    return new AsyncResultSetSupplier(
        commandContext,
        this,
        null,
        () ->
            keyspaceMetadata.isEmpty()
                ? Uni.createFrom()
                    .failure(
                        SchemaException.Code.INVALID_KEYSPACE.get(
                            Map.of("keyspace", schemaObject.name().keyspace())))
                : Uni.createFrom().item(new EmptyAsyncResultSet()));
  }
}
