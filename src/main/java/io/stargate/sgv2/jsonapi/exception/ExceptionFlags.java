package io.stargate.sgv2.jsonapi.exception;

/**
 * Declarative flags that capture the conditions observed while assembling an {@link APIException}.
 * Producers (for example {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler}) attach the
 * flags near the origin of the error, and higher layers (such as {@link
 * io.stargate.sgv2.jsonapi.service.processor.CommandProcessor}) decide which remediation steps to
 * perform.
 *
 * @see io.stargate.sgv2.jsonapi.service.processor.CommandProcessor#handleExceptionFlags
 * @see io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler
 */
public enum ExceptionFlags {
  /**
   * The flag indicates that the DB session used is no longer reliable and should be terminated
   * (e.g. after all cluster nodes restart), so that a fresh connection is created on the next
   * request.
   */
  UNRELIABLE_DB_SESSION,

  /**
   * This flag tells clients if they should retry the query, such as for a timeout from the DB. Not
   * yet used in the codebase but kept for future implementation.
   */
  RETRY
}
