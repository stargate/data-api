package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;

public record ApiClusteringDef(ApiColumnDef columnDef, ApiClusteringOrder order) {

  public ApiClusteringDef {
    if (columnDef == null) {
      throw new IllegalArgumentException("columnDef is required");
    }
    if (order == null) {
      throw new IllegalArgumentException("order is required");
    }
  }

  public static ApiClusteringDef from(ApiColumnDef columnDef, ClusteringOrder order) {
    return new ApiClusteringDef(columnDef, ApiClusteringOrder.from(order));
  }
}
