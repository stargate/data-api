package io.stargate.sgv2.jsonapi.service.provider;

import io.stargate.sgv2.jsonapi.exception.ExceptionHandler;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

/**
 * Interface for handling exception from an Embedding or Reranking model provider.
 *
 * <p>For now, we will keep both Embedding and Reranking in the one place until we know there are
 * different enough. See {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler} for more
 * details on how this works.
 *
 * <p>Used {@link Throwable} as the base exception class for now, because the providers ma have
 * different exception hierarchies. And quarkus / rest easy passes throwable for exceptions from the
 * rest client.
 *
 * <p><b>NOTE:</b> Subclass {@link DefaultProviderExceptionHandler} rather than implement this
 * interface directly.
 */
public interface ProviderExceptionHandler extends ExceptionHandler<Throwable> {

  @Override
  default Class<Throwable> getExceptionClass() {
    return Throwable.class;
  }

  @Override
  default Throwable handle(Throwable throwable) {
    return switch (throwable) {
      case TimeoutException e -> handle(e);
      case UnknownHostException e -> handle(e);
      default -> throwable;
    };
  }

  // ========================================================================
  // Build in Java exceptions, normally thrown by the quarkus / rest easy client
  // ========================================================================

  /** Vertex normally throws the NoStackTraceException which is a subclass. */
  default Throwable handle(TimeoutException exception) {
    return exception;
  }

  default Throwable handle(UnknownHostException exception) {
    return exception;
  }
}
