package io.stargate.sgv2.jsonapi.exception;

/**
 * Declarative actions that can be attached to an {@link APIException} instance to instruct higher
 * layers (for example {@code CommandProcessor}) to perform additional actions while handling the
 * error.
 *
 * <p>These actions are populated by the code that creates the exception (for example {@link
 * io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler}) and
 * interpreted when the exception is mapped to a response. This keeps remediation logic close to
 * where the error originates while still allowing generic handling at the API layer.
 *
 * @see io.stargate.sgv2.jsonapi.service.processor.CommandProcessor#handleExceptionActions
 * @see io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler
 */
public enum ExceptionAction {
  /**
   * Evict the session from cache. This is intended for use in scenarios where a session is known to
   * be in an unrecoverable state (e.g., after all cluster nodes restart) and needs to be forcibly
   * removed to allow for a fresh connection on the next request.
   */
  EVICT_SESSION_CACHE,

  RETRY
}
