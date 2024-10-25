package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;

public enum ApiClusteringOrder {
  ASC,
  DESC;

  public static ApiClusteringOrder from(ClusteringOrder order) {
    return switch (order) {
      case ASC -> ASC;
      case DESC -> DESC;
    };
  }
}
