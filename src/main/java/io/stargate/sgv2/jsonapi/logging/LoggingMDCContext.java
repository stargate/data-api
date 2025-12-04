package io.stargate.sgv2.jsonapi.logging;

/**
 * Simple interface for objects can update the Mapped Diagnostic Context (MDC) for logging purposes.
 *
 * <p>see https://quarkus.io/guides/logging#mdc-propagation
 */
public interface LoggingMDCContext {

  void addToMDC();

  void removeFromMDC();
}
