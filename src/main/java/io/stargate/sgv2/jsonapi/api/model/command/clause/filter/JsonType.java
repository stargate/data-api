package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/** Data type for a JSON value, used for de/encoding and storage. */
public enum JsonType {
  BOOLEAN,
  NUMBER,
  STRING,
  NULL,
  DATE,
  SUB_DOC,
  ARRAY,
  // DOCUMENT_ID represent the _id field type which is union of String, Number, Boolean and Null
  DOCUMENT_ID
}
