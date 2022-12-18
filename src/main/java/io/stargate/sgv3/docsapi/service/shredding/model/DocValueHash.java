package io.stargate.sgv3.docsapi.service.shredding.model;

/**
 * Value class that represents hash/digest of contents of a Document node (atomic value,
 * sub-document or array).
 *
 * <p>For atomic types calculated from String representation of the value (but not necessarily same
 * as what JSON uses) -- either type-prefixed value as-is (if short enough) or type-prefixed hash of
 * String representation (long String values).
 *
 * <p>For structured types (Objects, Arrays), calculated from hashes/digest of directly contained
 * values: this will effectively calculated recursive hash/digest.
 */
public record DocValueHash(String hash) {
  @Override
  public String toString() {
    return hash;
  }
}
