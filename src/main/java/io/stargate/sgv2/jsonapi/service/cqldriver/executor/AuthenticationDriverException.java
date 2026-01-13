package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;

/**
 * Exception to carry a {@link AuthenticationException} from the driver which is not a subclass of
 * the DriverException. Needs to be a subclass for the {@link DriverExceptionHandler} to handle it
 */
public class AuthenticationDriverException extends APIDriverException {

  private final EndPoint endPoint;

  AuthenticationDriverException(EndPoint endPoint, String message, Throwable cause) {
    super(message, null, cause, false);
    this.endPoint = endPoint;
  }

  public static AuthenticationDriverException from(AuthenticationException exception) {
    return new AuthenticationDriverException(
        exception.getEndPoint(), exception.getMessage(), exception.getCause());
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  @Override
  public DriverException copy() {
    return new AuthenticationDriverException(endPoint, getMessage(), getCause());
  }
}
