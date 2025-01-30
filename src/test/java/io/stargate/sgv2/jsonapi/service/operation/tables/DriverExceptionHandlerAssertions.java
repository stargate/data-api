package io.stargate.sgv2.jsonapi.service.operation.tables;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/**
 * @param <FixtureT>
 * @param <SchemaT>
 */
public class DriverExceptionHandlerAssertions<FixtureT, SchemaT extends SchemaObject> {

  private final FixtureT fixture;
  private final DefaultDriverExceptionHandler.Factory<SchemaT> handlerFactory;

  // We set this in getHandlerFactory() where it wraps the original factory
  private DriverExceptionHandler target;

  public DriverExceptionHandlerAssertions(
      FixtureT fixture, DefaultDriverExceptionHandler.Factory<SchemaT> handlerFactory) {
    this.fixture = fixture;
    this.handlerFactory = handlerFactory;
  }

  public DefaultDriverExceptionHandler.Factory<SchemaT> getHandlerFactory() {
    return (SchemaT schemaObject, SimpleStatement statement) -> {
      // do not think we should be calling this multiple times
      if (target != null) {
        throw new IllegalStateException("ErrorHandler Factory already called");
      }

      target = spy(handlerFactory.apply(schemaObject, statement));
      return target;
    };
  }

  /** Map the assertions exception to a returned exception */
  public FixtureT doMaybeHandleException(RuntimeException expected, RuntimeException returned) {

    when(target.maybeHandle(eq(expected))).thenReturn(returned);

    return fixture;
  }

  /** Always return the same exception */
  public FixtureT doMaybeHandleException(RuntimeException returned) {

    when(target.maybeHandle(any())).thenReturn(returned);

    return fixture;
  }

  public FixtureT verifyMaybeHandleOnce(RuntimeException exception, String msg) {
    verify(target, times(1).description("maybeHandle() called %s times when: %s".formatted(1, msg)))
        .maybeHandle(eq(exception));
    return fixture;
  }
}
