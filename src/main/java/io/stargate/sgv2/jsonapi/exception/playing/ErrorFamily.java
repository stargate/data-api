package io.stargate.sgv2.jsonapi.exception.playing;

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
