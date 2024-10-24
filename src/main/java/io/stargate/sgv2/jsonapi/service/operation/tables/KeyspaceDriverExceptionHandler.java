package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTableCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import java.util.Map;

public class KeyspaceDriverExceptionHandler
    extends DefaultDriverExceptionHandler<KeyspaceSchemaObject> {

  private Command command;

  public KeyspaceDriverExceptionHandler(Command command) {
    super();
    this.command = command;
  }

  @Override
  public RuntimeException handle(
      KeyspaceSchemaObject schemaObject, InvalidQueryException exception) {
    // Custom handling for DropIndexCommand and index not found
    if (command instanceof DropIndexCommand && exception.getMessage().contains("doesn't exist")) {
      return SchemaException.Code.INDEX_NOT_FOUND.get(Map.of("reason", exception.getMessage()));
    }

    // Custom handling for DropTableCommand and index not found
    if (command instanceof DropTableCommand && exception.getMessage().contains("doesn't exist")) {
      return SchemaException.Code.TABLE_NOT_FOUND.get(Map.of("reason", exception.getMessage()));
    }

    return super.handle(schemaObject, exception);
  }

  @Override
  public RuntimeException handle(
      KeyspaceSchemaObject schemaObject, AlreadyExistsException exception) {

    // Custom handling for CreateTableCommand and table already exists
    if (command instanceof CreateTableCommand
        && exception.getMessage().contains("already exists")) {
      return SchemaException.Code.TABLE_ALREADY_EXISTS.get(
          Map.of("reason", exception.getMessage()));
    }

    return super.handle(schemaObject, exception);
  }
}
