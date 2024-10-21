package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;

/** Builds a {@link DropTableAttempt}. */
public class DropTableAttemptBuilder {
  private int position;
  private final KeyspaceSchemaObject schemaObject;
  private CqlIdentifier name;
  private boolean ifExists;

  public DropTableAttemptBuilder(KeyspaceSchemaObject schemaObject) {
    this.schemaObject = schemaObject;
  }

  public DropTableAttemptBuilder withName(String name) {
    this.name = CqlIdentifierUtil.cqlIdentifierFromUserInput(name);
    return this;
  }

  public DropTableAttemptBuilder withIfExists(boolean ifExists) {
    this.ifExists = ifExists;
    return this;
  }

  public DropTableAttempt build() {
    return new DropTableAttempt(position++, schemaObject, name, ifExists);
  }
}
