package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.PrimaryKeyDesc;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;
import java.util.Optional;

public class ApiClusteringDef implements Recordable {

  public static final FromUserDescFactory FROM_USER_DESC_FACTORY = new FromUserDescFactory();
  public static final FromCqlFactory FROM_CQL_FACTORY = new FromCqlFactory();

  private final ApiColumnDef columnDef;
  private final ApiClusteringOrder order;

  public ApiClusteringDef(ApiColumnDef columnDef, ApiClusteringOrder order) {
    this.columnDef = Objects.requireNonNull(columnDef, "columnDef cannot be null");
    this.order = Objects.requireNonNull(order, "order cannot be null");
  }

  public ApiColumnDef columnDef() {
    return columnDef;
  }

  public ApiClusteringOrder order() {
    return order;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder.append("order", order).append("columnDef", columnDef);
  }

  public static class FromUserDescFactory extends FactoryFromDesc {

    public Optional<ApiClusteringDef> create(
        ApiColumnDefContainer columns, PrimaryKeyDesc.OrderingKeyDesc orderingKeyDesc) {

      Objects.requireNonNull(columns, "columns must not be null");
      Objects.requireNonNull(orderingKeyDesc, "orderingKeyDesc must not be null");

      var columnDef = columns.get(userNameToIdentifier(orderingKeyDesc.column(), "clusteringKey"));
      return columnDef == null
          ? Optional.empty()
          : Optional.of(
              new ApiClusteringDef(columnDef, ApiClusteringOrder.from(orderingKeyDesc.order())));
    }
  }

  public static class FromCqlFactory {
    public ApiClusteringDef create(ApiColumnDef columnDef, ClusteringOrder order) {
      return new ApiClusteringDef(columnDef, ApiClusteringOrder.from(order));
    }
  }
}
