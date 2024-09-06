package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.exception.playing.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import io.stargate.sgv2.jsonapi.exception.playing.DatabaseException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

public class TableDriverExceptionHandler extends DefaultDriverExceptionHandler<TableSchemaObject> {

  @Override
  public RuntimeException handle(TableSchemaObject schemaObject, WriteTimeoutException exception) {
    return DatabaseException.Code.TABLE_WRITE_TIMEOUT.get(
        errVars(
            schemaObject,
            m -> {
              m.put("blockFor", String.valueOf(exception.getBlockFor()));
              m.put("received", String.valueOf(exception.getReceived()));
            }));
  }
}
