package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;

/**
 * Subclass of {@link DefaultDriverExceptionHandler} for working with {@link TableSchemaObject}.
 *
 * <p>The class may be used directly when working with a Table and there are no specific exception
 * handling for the command, or it may be subclassed by exception handlers for a command that have
 * specific exception handling such as for {@link CreateIndexExceptionHandler}
 */
public class TableDriverExceptionHandler
    extends DefaultDriverExceptionHandler<TableBasedSchemaObject> {

  public TableDriverExceptionHandler(
      TableBasedSchemaObject schemaObject, SimpleStatement statement) {
    super(schemaObject, statement);
  }
}
