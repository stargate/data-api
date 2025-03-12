package io.stargate.sgv2.jsonapi.service.operation.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.shredding.NamedValue;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A CQL clause that adds an ORDER BY clause to a SELECT for clustering keys
 *
 * <p>Note: Does not check if the columns are clustering columns, or that the order is correct that
 * is up to the builder.
 */
public class TableOrderByClusteringCqlClause implements OrderByCqlClause {

  public enum Order {
    ASC(ClusteringOrder.ASC),
    DESC(ClusteringOrder.DESC);

    public final ClusteringOrder cqlOrder;

    Order(ClusteringOrder cqlOrder) {
      this.cqlOrder = cqlOrder;
    }
  }

  private final List<OrderByTerm> orderByTerms;

  public TableOrderByClusteringCqlClause(List<OrderByTerm> orderByTerms) {
    this.orderByTerms =
        Collections.unmodifiableList(
            Objects.requireNonNull(orderByTerms, "orderByTerms must not be null"));

    // sanity checks
    if (orderByTerms.isEmpty()) {
      throw new IllegalArgumentException("orderByTerms must not be empty");
    }
    // assuming that the builder of the class has checked things like this is a clustering column
  }

  @Override
  public Select apply(Select select) {
    for (OrderByTerm orderByTerm : orderByTerms) {
      select = select.orderBy(orderByTerm.apiColumnDef.name(), orderByTerm.order.cqlOrder);
    }
    return select;
  }

  @Override
  public boolean fullyCoversCommand() {
    return true;
  }

  @Override
  public List<? extends NamedValue<?, ?, ?>> deferred() {
    // never have deferred values when doing regular ASC or DESC sorting
    return List.of();
  }

  @Override
  public String toString() {
    return "TableOrderByClusteringCqlClause{" + "orderByTerms=" + orderByTerms + '}';
  }

  public record OrderByTerm(ApiColumnDef apiColumnDef, Order order) {
    public OrderByTerm {
      Objects.requireNonNull(apiColumnDef, "apiColumnDef must not be null");
      Objects.requireNonNull(order, "order must not be null");
    }

    @Override
    public String toString() {
      return "OrderByTerm{"
          + "apiColumnDef.name="
          + cqlIdentifierToMessageString(apiColumnDef.name())
          + ", order="
          + order
          + '}';
    }
  }
}
