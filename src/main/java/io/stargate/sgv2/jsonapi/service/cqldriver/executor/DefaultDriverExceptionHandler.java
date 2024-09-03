package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.playing.ErrorFormatters.errFmt;

import com.datastax.oss.driver.api.core.DriverException;
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
 * <p>Subclasses should only override the <code>handle()</code> functions they need to and let the
 * defaults in this class handle the rest. Also, the {@link
 * DriverExceptionHandler#handleUnhandled(SchemaObject, DriverException)} will kick in if there is
 * no handler for the specific error type.
 *
 * <p><b>NOTE:</b> Try to keep the <code>handle()</code> functions grouped like they are in the
 * interface.
 *
 * @param <T> The type of schema object this handler is for.
 */
public class DefaultDriverExceptionHandler<T extends SchemaObject>
    implements DriverExceptionHandler<T> {

  // ========================================================================
  // Direct subclasses of DriverException with no child
  // ========================================================================

  @Override
  public RuntimeException handle(T schemaObject, ClosedConnectionException exception) {
    return DatabaseException.Code.CLOSED_CONNECTION.get(
        errFmt(schemaObject, map -> map.put("errorMessage", exception.getMessage())));
  }
}
