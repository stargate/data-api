package io.stargate.sgv2.jsonapi.service.operation.collections;

/** Controls whether an internal read returns one page or a bounded candidate set. */
public enum CollectionReadMode {
  SINGLE_PAGE,
  CANDIDATES
}
