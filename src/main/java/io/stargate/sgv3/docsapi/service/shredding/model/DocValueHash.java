package io.stargate.sgv3.docsapi.service.shredding.model;

/**
 * Value class that represents hash/digest of contents of a Document node (atomic value,
 * sub-document or array), calculated recursively for the whole contents.
 */
public record DocValueHash(String hash) {
  @Override
  public String toString() {
    return hash;
  }
}
