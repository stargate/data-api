package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;

/**
 * Marker class for all situations where we need to remap exceptions from the driver that are not
 * subclasses of DriverException.
 */
public abstract class APIDriverException extends DriverException {

  protected APIDriverException(
      String message, ExecutionInfo executionInfo, Throwable cause, boolean writableStackTrace) {
    super(message, executionInfo, cause, writableStackTrace);
  }
}
