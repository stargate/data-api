package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.EmptyAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.Task;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

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
  protected KeyspaceMetadata keyspaceMetadata;

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

    return new MetadataAsyncResultSetSupplier(
        commandContext,
        this,
        null,
        queryExecutor,
        schemaObject.identifier().keyspace());
//    this.keyspaceMetadata = queryExecutor.getKeyspaceMetadata(schemaObject.identifier().keyspace());
//
//    // TODO: BETTER ERROR
//    return new AsyncResultSetSupplier(
//        commandContext,
//        this,
//        null,
//        () ->
//            queryExecutor
//                .getKeyspaceMetadata(schemaObject.identifier().keyspace())
//
//            keyspaceMetadata.isEmpty()
//                ? Uni.createFrom()
//                    .failure(
//                        SchemaException.Code.INVALID_KEYSPACE.get(
//                            Map.of("keyspace", schemaObject.identifier().keyspace())))
//                : Uni.createFrom().item(new EmptyAsyncResultSet()));
  }

  @Override
  protected void onSuccess(AsyncResultSetSupplier resultSupplier, AsyncResultSet result) {
    super.onSuccess(resultSupplier, result);

    this.keyspaceMetadata = ((MetadataAsyncResultSetSupplier) resultSupplier).keyspaceMetadata;
  }

  static class MetadataAsyncResultSetSupplier extends  AsyncResultSetSupplier{

    KeyspaceMetadata keyspaceMetadata;

    private final CqlIdentifier keyspaceName;
    private final CommandQueryExecutor queryExecutor;

    MetadataAsyncResultSetSupplier(
        CommandContext<?> commandContext,
        Task<?> task,
        SimpleStatement statement,
        CommandQueryExecutor queryExecutor,
        CqlIdentifier keyspaceName) {
      super(commandContext, task, statement, null);
      this.queryExecutor = queryExecutor;
      this.keyspaceName = keyspaceName;
    }

    @Override
    protected UniSupplier<AsyncResultSet> getSupplier() {

      return () ->
          queryExecutor
              .getKeyspaceMetadata(keyspaceName, false)
              .flatMap(optMetadata ->{
                this.keyspaceMetadata = optMetadata.orElse(null);

                return optMetadata.isEmpty() ?
                    Uni.createFrom().failure(SchemaException.Code.INVALID_KEYSPACE.get(Map.of("keyspace", cqlIdentifierToMessageString(keyspaceName) )))
                    :
                    Uni.createFrom().item(new EmptyAsyncResultSet());
              });
    }
  }
}
