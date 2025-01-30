package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import org.junit.jupiter.api.Test;

/** Tests for {@link DriverExceptionHandler} interface only */
public class DriverExceptionHandlerTest {

  @Test
  public void callWithNonDriverReturnsSame() {

    var originalEx = new RuntimeException("original");

    // Not using mocks because want all the defaults in the interface to kick in
    var handler = new DriverExceptionHandler() {};
    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(originalEx));

    assertThat(actualEx)
        .as("When not a DriverException, should return the same exception")
        .isSameAs(originalEx);
  }

  @Test
  public void callsWriteTimeoutException() {

    var originalEx =
        new WriteTimeoutException(null, ConsistencyLevel.QUORUM, 1, 2, WriteType.SIMPLE);
    var expectedEx = new RuntimeException("expected");

    // Not using mocks because want all the defaults in the interface to kick in
    final Object[] calledWith = new Object[1];
    var handler =
        new DriverExceptionHandler() {
          public RuntimeException handle(WriteTimeoutException exception) {
            calledWith[0] = exception;
            return expectedEx;
          }
        };

    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(originalEx));

    assertThat(actualEx)
        .as("When a WriteFailureException, should return the exception from the handler")
        .isSameAs(expectedEx);

    assertThat(calledWith[0])
        .as("Should have called the handler with the original exception")
        .isSameAs(originalEx);
  }

  @Test
  public void defaultDriverErrorHandled() {

    var originalEx =
        new WriteTimeoutException(null, ConsistencyLevel.QUORUM, 1, 2, WriteType.SIMPLE);

    // Not using mocks because want all the defaults in the interface to kick in
    var handler = new DriverExceptionHandler() {};

    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(originalEx));

    assertThat(actualEx)
        .as(
            "When a DriverException and no handler should return ServerException code=%s",
            DatabaseException.Code.UNEXPECTED_DRIVER_ERROR.name())
        .isNotNull()
        .isInstanceOf(ServerException.class)
        .hasMessageContaining(WriteTimeoutException.class.getSimpleName())
        .hasMessageContaining(originalEx.getMessage())
        .satisfies(
            e -> {
              var apiError = (APIException) e;
              assertThat(apiError.code)
                  .isEqualTo(DatabaseException.Code.UNEXPECTED_DRIVER_ERROR.name());
            });
  }
}
