package io.stargate.sgv2.jsonapi.service.operation.model.collections;

/**
 * Read type specifies what data needs to be read and returned as part of the response for
 * operations
 */
public enum CollectionReadType {
  /**
   * Return documents and transaction id which satisfies the filter conditions as part of response
   */
  DOCUMENT,
  /** Return sorted documents which satisfies the filter conditions as part of response */
  SORTED_DOCUMENT,
  /**
   * Return only document id and transaction id of documents which satisfies the filter conditions
   * as part of response
   */
  KEY,
}
