package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import java.util.ArrayList;
import java.util.List;

/** TODO = NOT needed to test the BaseTask, only for the DBTask */
public class DriverExceptionHandlerAssertions<FixtureT, SchemaT extends SchemaObject> {

  private final FixtureT fixture;
  private final DefaultDriverExceptionHandler.Factory<SchemaT> handlerFactory;

  // We set this in getHandlerFactory() where it wraps the original factory
  private DriverExceptionHandler target;

  private List<HandlePair> handlePairs = new ArrayList<>();

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
      handlePairs.forEach(pair -> pair.apply(target));
      return target;
    };
  }

  /** Map the assertions exception to a returned exception */
  public FixtureT doMaybeHandleException(RuntimeException expected, RuntimeException returned) {

    // do not have the target handler yet, need to wait until the factory is called
    handlePairs.add(new HandlePair(expected, returned));
    return fixture;
  }

  /** Always return the same exception */
  public FixtureT doMaybeHandleException(RuntimeException returned) {

    // do not have the target handler yet, need to wait until the factory is called
    handlePairs.add(new HandlePair(null, returned));
    return fixture;
  }

  public FixtureT verifyMaybeHandleOnce(RuntimeException exception, String msg) {
    verify(target, times(1).description("maybeHandle() called %s times when: %s".formatted(1, msg)))
        .maybeHandle(eq(exception));
    return fixture;
  }

  private record HandlePair(RuntimeException expected, RuntimeException returned) {
    void apply(DriverExceptionHandler target) {
      if (expected == null) {
        when(target.maybeHandle(any())).thenReturn(returned);
      } else {
        when(target.maybeHandle(eq(expected))).thenReturn(returned);
      }
    }
  }
}
