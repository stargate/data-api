package io.stargate.sgv2.jsonapi.exception;

/**
 * Top level hierarchy for all exceptions thrown by the API.
 *
 * <p>See {@link APIException}
 */
public enum ErrorFamily {
  /** See {@link RequestException} */
  REQUEST,

  /** See {@link ServerException} */
  SERVER;
}
