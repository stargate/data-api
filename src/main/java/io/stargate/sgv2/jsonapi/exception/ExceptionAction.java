package io.stargate.sgv2.jsonapi.exception;

public enum ExceptionAction {
  /**
   * Evict the session from cache. This is intended for use in scenarios where a session is known to
   * be in an unrecoverable state (e.g., after all cluster nodes restart) and needs to be forcibly
   * removed to allow for a fresh connection on the next request.
   */
  EVICT_SESSION_CACHE,

  RETRY
}
