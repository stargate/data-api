package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/** Data type for a type-tagged JSON value, used for de/encoding and storage. */
public enum JsonType {
  BOOLEAN,
  NUMBER,
  STRING,
  NULL,
  /**
   * Stored as a special EJSON-encoded JSON Object (1 entry with key "$date"); used by Collections
   */
  DATE,
  /** JSON Objects other than EJSON-encoded special types */
  SUB_DOC,
  ARRAY,
  /**
   * DOCUMENT_ID represents the _id field type which is union of String, Number, Boolean, Date and
   * Null (separate type due to use as Cassandra partition key; tuple of TINYINT and String)
   */
  DOCUMENT_ID,
  /**
   * Type created from single-entry JSON Object with a key starting with "$" (e.g. "$blob", "$date")
   * to store special types in JSON (e.g. binary data, dates). Used as an intermediate type for
   * further processing.
   *
   * <p>NOTE: only used by Tables processing; not used with Collections.
   */
  EJSON_WRAPPER
}
