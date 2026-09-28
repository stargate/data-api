package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import java.util.Objects;

/**
 * Encodes Data API {@code _id} values into the Cassandra composite primary-key format used by HCD
 * OpenSearch indexing.
 *
 * <p>For collections, the partition key is always {@code 1} (fixed integer for shredded
 * collections) and the clustering key is the document {@code _id}. Thus the encoded format in
 * OpenSearch document metadata is {@code {_0=1, _1=<dataApiId>}}.
 */
public final class OpenSearchIdEncoder {

  private OpenSearchIdEncoder() {}

  /**
   * Encodes a Data API string {@code _id} into the HCD composite-key string format.
   *
   * @param dataApiId the document ID as provided in the Data API
   * @return the composite key string (e.g., {@code {_0=1, _1=product-3}})
   */
  public static String encode(String dataApiId) {
    Objects.requireNonNull(dataApiId, "dataApiId must not be null");
    return "{_0=1, _1=" + dataApiId + "}";
  }
}
