package io.stargate.sgv2.jsonapi.service.operation.model;

/**
 * Read type specifies what data needs to be read and returned as part of the response for
 * operations
 */
public enum ReadType {
  /**
   * Return documents and transaction id which satisfies the filter conditions as part of response
   */
  DOCUMENT,
  /**
   * Return only document id and transaction id of documents which satisfies the filter conditions
   * as part of response
   */
  KEY,
  /** Return only count of documents which satisfies the filter condition */
  COUNT
}
