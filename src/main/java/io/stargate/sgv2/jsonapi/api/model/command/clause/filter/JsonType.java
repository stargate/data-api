package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/** Data type for a JSON value, used for de/encoding and storage. */
public enum JsonType {
  BOOLEAN,
  NUMBER,
  STRING,
  NULL,
  SUB_DOC,
  ARRAY
}
