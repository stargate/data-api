package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;

/*
 An attempt to drop type in a keyspace.
*/
public class DropTypeDBTask extends SchemaDBTask<KeyspaceSchemaObject> {

  private final CqlIdentifier name;
  private final CQLOptions<Drop> cqlOptions;

  public DropTypeDBTask(
      int position,
      KeyspaceSchemaObject schemaObject,
      SchemaRetryPolicy schemaRetryPolicy,
      DefaultDriverExceptionHandler.Factory<KeyspaceSchemaObject> exceptionHandlerFactory,
      CqlIdentifier name,
      CQLOptions<Drop> cqlOptions) {
    super(position, schemaObject, schemaRetryPolicy, exceptionHandlerFactory);
    this.name = name;
    this.cqlOptions = cqlOptions;
    setStatus(TaskStatus.READY);
  }

  public static DropTypeDBTaskBuilder builder(KeyspaceSchemaObject schemaObject) {
    return new DropTypeDBTaskBuilder(schemaObject);
  }

  @Override
  protected SimpleStatement buildStatement() {
    CqlIdentifier keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());

    Drop drop = SchemaBuilder.dropType(keyspaceIdentifier, name);

    // Apply any additional options
    drop = cqlOptions.applyBuilderOptions(drop);

    return drop.build();
  }
}
