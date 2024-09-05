package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.playing.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import io.stargate.sgv2.jsonapi.exception.playing.DatabaseException;

/**
 * Default implementation of the {@link DriverExceptionHandler} interface, we keep the interface so
 * all the type casting is done in one place and this class only worries about processing the
 * errors.
 *
 * <p>This class should cover almost all the driver exceptions, we create subclasses like the {@link
 * io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler} to handle errors
 * in a table specific (or other schema object) way. e.g. if we have different error codes for a
 * timeout for a table and a collection.
 *
 * <p><b>NOTE:</b> Try to keep the <code>handle()</code> functions grouped like they are in the
 * interface.
 *
 * @param <SchemaT> The type of schema object this handler is for.
 */
public class DefaultDriverExceptionHandler<SchemaT extends SchemaObject>
    implements DriverExceptionHandler<SchemaT> {

  // ========================================================================
  // Direct subclasses of DriverException with no child
  // ========================================================================

  @Override
  public RuntimeException handle(SchemaT schemaObject, ClosedConnectionException exception) {
    return DatabaseException.Code.CLOSED_CONNECTION.get(
        errFmt(schemaObject, map -> map.put("errorMessage", exception.getMessage())));
  }
}
