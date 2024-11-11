package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKeyDesc;
import java.util.Objects;

public enum ApiClusteringOrder {
  ASC(ClusteringOrder.ASC, PrimaryKeyDesc.OrderingKeyDesc.Order.ASC),
  DESC(ClusteringOrder.DESC, PrimaryKeyDesc.OrderingKeyDesc.Order.DESC);

  private ClusteringOrder cqlOrder;
  private PrimaryKeyDesc.OrderingKeyDesc.Order orderDesc;

  ApiClusteringOrder(ClusteringOrder cqlOrder, PrimaryKeyDesc.OrderingKeyDesc.Order orderDesc) {
    this.cqlOrder = cqlOrder;
    this.orderDesc = orderDesc;
  }

  public ClusteringOrder getCqlOrder() {
    return cqlOrder;
  }

  public PrimaryKeyDesc.OrderingKeyDesc.Order getOrderDesc() {
    return orderDesc;
  }

  public static ApiClusteringOrder from(ClusteringOrder order) {
    Objects.requireNonNull(order, "order must not be null");
    return switch (order) {
      case ASC -> ASC;
      case DESC -> DESC;
    };
  }

  public static ApiClusteringOrder from(PrimaryKeyDesc.OrderingKeyDesc.Order order) {
    Objects.requireNonNull(order, "order must not be null");

    return switch (order) {
      case ASC -> ASC;
      case DESC -> DESC;
    };
  }
}
