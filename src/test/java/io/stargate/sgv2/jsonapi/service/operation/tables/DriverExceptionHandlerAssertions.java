package io.stargate.sgv2.jsonapi.service.operation.tables;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

public class DriverExceptionHandlerAssertions<FixtureT, SchemaT extends SchemaObject> {

  private final FixtureT fixture;
  private final DriverExceptionHandler<SchemaT> target;

  public DriverExceptionHandlerAssertions(
      FixtureT fixture, DriverExceptionHandler<SchemaT> target) {
    this.fixture = fixture;
    this.target = target;
  }

  /** Map the expected exception to a returned exception */
  public FixtureT doMaybeHandleException(RuntimeException expected, RuntimeException returned) {

    when(target.maybeHandle(any(), eq(expected))).thenReturn(returned);

    return fixture;
  }

  /** Always return the same exception */
  public FixtureT doMaybeHandleException(RuntimeException returned) {

    when(target.maybeHandle(any(), any())).thenReturn(returned);

    return fixture;
  }

  public FixtureT verifyMaybeHandleOnce(RuntimeException exception, String msg) {
    verify(target, times(1).description("maybeHandle() called %s times when: %s".formatted(1, msg)))
        .maybeHandle(any(), eq(exception));
    return fixture;
  }
}
