package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

/** Builds a {@link DropIndexAttempt}. */
public class DropIndexAttemptBuilder {
  private int position;
  private final KeyspaceSchemaObject schemaObject;
  private CqlIdentifier name;
  private boolean ifExists;

  public DropIndexAttemptBuilder(KeyspaceSchemaObject schemaObject) {
    this.schemaObject = schemaObject;
  }

  public DropIndexAttemptBuilder withName(String name) {
    this.name = CqlIdentifierUtil.cqlIdentifierFromUserInput(name);
    return this;
  }

  public DropIndexAttemptBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropIndexAttempt build() {
    return new DropIndexAttempt(position++, schemaObject, name, ifExists);
  }
}
