package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;

/*
 An attempt to drop index for a column.
*/
public class DropIndexAttempt extends SchemaAttempt<KeyspaceSchemaObject> {
  private final CqlIdentifier name;
  private final boolean ifExists;

  public DropIndexAttempt(
      int position,
      KeyspaceSchemaObject schemaObject,
      CqlIdentifier name,
      boolean ifExists,
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    super(position, schemaObject, schemaRetryPolicy);
    this.name = name;
    this.ifExists = ifExists;
    setStatus(OperationStatus.READY);
  }

  @Override
  protected SimpleStatement buildStatement() {
    CqlIdentifier keyspaceIdentifier = cqlIdentifierFromUserInput(schemaObject.name().keyspace());

    // Set as StorageAttachedIndex as default
    Drop drop = SchemaBuilder.dropIndex(keyspaceIdentifier, name);

    if (ifExists) {
      drop = drop.ifExists();
    }

    return drop.build();
  }
}
