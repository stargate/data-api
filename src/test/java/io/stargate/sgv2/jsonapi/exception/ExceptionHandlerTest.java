package io.stargate.sgv2.jsonapi.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import java.nio.channels.WritePendingException;
import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link ExceptionHandler} interface only.
 *
 * <p>Not using mocks because want all the defaults in the interface to kick in
 */
public class ExceptionHandlerTest {

  @Test
  public void handleNull() {

    var handler =
        new ExceptionHandler<UnsupportedOperationException>() {
          @Override
          public Class<UnsupportedOperationException> getExceptionClass() {
            return UnsupportedOperationException.class;
          }
        };
    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(null));

    assertThat(actualEx).as("When handling null, returns null").isNull();
  }

  @Test
  public void handleNoneBaseTypeReturnsSame() {

    var originalEx = new IllegalStateException("original");

    // UnsupportedOperationException and IllegalStateException are both direct subclasses of
    // RuntimeException
    // a handler for one should not be called for the other
    var handler =
        new ExceptionHandler<UnsupportedOperationException>() {
          @Override
          public Class<UnsupportedOperationException> getExceptionClass() {
            return UnsupportedOperationException.class;
          }
        };
    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(originalEx));

    assertThat(actualEx)
        .as("When handling non BaseT exception, returns the exception object passed")
        .isSameAs(originalEx);
  }

  /**
   * IllegalStateException is a subclass of RuntimeException, and WritePendingException is a
   * subclass of IllegalStateException. A handler for IllegalStateException should handle both of
   * them correctly.
   */
  @Test
  public void handleHierarchyType() {

    var originalParentEx = new IllegalStateException("original parent");
    var originalChildEx = new WritePendingException();
    var expectedParentEx = new RuntimeException("expected parent");
    var expectedChildEx = new RuntimeException("expected child");

    final Object[] calledWith = new Object[2];
    var handler =
        new ExceptionHandler<IllegalStateException>() {
          @Override
          public Class<IllegalStateException> getExceptionClass() {
            return IllegalStateException.class;
          }

          @Override
          public RuntimeException handle(IllegalStateException exception) {
            if (exception instanceof WritePendingException writePendingException) {
              return handle(writePendingException);
            }
            calledWith[0] = exception;
            return expectedParentEx;
          }

          public RuntimeException handle(WritePendingException exception) {
            calledWith[1] = exception;
            return expectedChildEx;
          }
        };

    // First test, with the parent exception
    var actualParentTest = assertDoesNotThrow(() -> handler.maybeHandle(originalParentEx));

    assertThat(actualParentTest)
        .as(
            "When handling BaseT exception, returns object from handler() for BaseT - exception=%s",
            originalParentEx)
        .isSameAs(expectedParentEx);

    assertThat(calledWith[0])
        .as("When handling BaseT exception, passes the original exception to handler() for BaseT")
        .isSameAs(originalParentEx);

    assertThat(calledWith[1])
        .as(
            "When handling BaseT exception, does not pass the original exception to handler() for ChildT")
        .isNull();

    // reset to run with the child ex
    calledWith[0] = null;
    calledWith[1] = null;

    // Second test, with the child exception
    var actualChildTest = assertDoesNotThrow(() -> handler.maybeHandle(originalChildEx));

    assertThat(actualChildTest)
        .as(
            "When handling ChildT exception, returns object from handler() for ChildT - exception=%s",
            originalChildEx)
        .isSameAs(expectedChildEx);

    assertThat(calledWith[0])
        .as(
            "When handling BaseT exception, does not pass the original exception to handler() for BaseT")
        .isNull();

    assertThat(calledWith[1])
        .as("When handling BaseT exception, passes the original exception to handler() for ChildT")
        .isSameAs(originalChildEx);
  }

  /**
   * If no error handler called, returns a {@link APIException} with code {@link
   * ServerException.Code#UNEXPECTED_SERVER_ERROR}
   */
  @Test
  public void defaultErrorHandler() {

    var originalEx =
        new WriteTimeoutException(null, ConsistencyLevel.QUORUM, 1, 2, WriteType.SIMPLE);

    // Not using mocks because want all the defaults in the interface to kick in
    var handler =
        new ExceptionHandler<DriverException>() {
          @Override
          public Class<DriverException> getExceptionClass() {
            return DriverException.class;
          }
        };

    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(originalEx));

    assertThat(actualEx)
        .as(
            "When no handlers for original exceptions default handled to ServerException code=%s",
            ServerException.Code.UNEXPECTED_SERVER_ERROR.name())
        .isNotNull()
        .isInstanceOf(ServerException.class)
        .hasMessageContaining(originalEx.getClass().getSimpleName())
        .hasMessageContaining(originalEx.getMessage())
        .satisfies(
            e -> {
              var apiError = (APIException) e;
              assertThat(apiError.code)
                  .isEqualTo(ServerException.Code.UNEXPECTED_SERVER_ERROR.name());
              assertThat(apiError.exceptionFlags)
                  .as("Default error should have empty exception flags")
                  .isEmpty();
            });
  }

  /**
   * If called with an {@link APIException}, it should return the same instance if it is not handled
   * by the implementation.
   */
  @Test
  public void handleAPIExceptionReturnsSameInstance() {

    var originalEx = ServerException.Code.INTERNAL_SERVER_ERROR.get("errorMessage", "testing");

    // No handler for any subclasses, but this handler can handle RuntimeException, which
    // APIException is
    var handler =
        new ExceptionHandler<RuntimeException>() {
          @Override
          public Class<RuntimeException> getExceptionClass() {
            return RuntimeException.class;
          }
        };
    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(originalEx));

    assertThat(actualEx)
        .as(
            "When handling non BaseT exception, of APIException returns the exception object passed")
        .isSameAs(originalEx);
  }

  /**
   * If called with an {@link APIException}, and the {@link
   * ExceptionHandler#ignoreUnhandledApiException()} == false then we should wrap with a new
   * instance
   */
  @Test
  public void handleAPIExceptionReturnsDiffInstance() {

    var originalEx = ServerException.Code.INTERNAL_SERVER_ERROR.get("errorMessage", "testing");

    // No handler for any subclasses, but this handler can handle RuntimeException, which
    // APIException is
    var handler =
        new ExceptionHandler<RuntimeException>() {
          @Override
          public Class<RuntimeException> getExceptionClass() {
            return RuntimeException.class;
          }

          @Override
          public boolean ignoreUnhandledApiException() {
            return false;
          }
        };
    var actualEx = assertDoesNotThrow(() -> handler.maybeHandle(originalEx));

    assertThat(actualEx)
        .as(
            "When handling non BaseT exception, of APIException wraps when ignoreUnhandledApiException==false")
        .isNotSameAs(originalEx);
  }
}
