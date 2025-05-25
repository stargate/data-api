package io.stargate.sgv2.jsonapi.logging;

/**
 * see https://quarkus.io/guides/logging#mdc-propagation
 */
public interface LoggingMDCContext {

  void addToMDC();

  void removeFromMDC();
}
