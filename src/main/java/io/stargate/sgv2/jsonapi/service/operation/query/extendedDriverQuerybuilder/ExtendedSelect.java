package io.stargate.sgv2.jsonapi.service.operation.query.extendedDriverQuerybuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;

public class ExtendedSelect extends DefaultSelect {

  public ExtendedSelect(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    super(keyspace, table);
  }

  @NonNull
  public static SelectFrom selectFrom(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    return new ExtendedSelect(keyspace, table);
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    builder.append("SELECT");
    if (isJson()) {
      builder.append(" JSON");
    }
    if (isDistinct()) {
      builder.append(" DISTINCT");
    }

    CqlHelper.append(getSelectors(), builder, " ", ",", null);

    builder.append(" FROM ");
    CqlHelper.qualify(getKeyspace(), getTable(), builder);

    DefaultSubConditionRelation.appendWhereClause(builder, getRelations(), false);

    CqlHelper.append(getGroupByClauses(), builder, " GROUP BY ", ",", null);

    boolean first = true;
    for (Map.Entry<CqlIdentifier, ClusteringOrder> entry : getOrderings().entrySet()) {
      if (first) {
        builder.append(" ORDER BY ");
        first = false;
      } else {
        builder.append(",");
      }
      builder.append(entry.getKey().asCql(true)).append(" ").append(entry.getValue().name());
    }

    if (getLimit() != null) {
      builder.append(" LIMIT ");
      if (getLimit() instanceof BindMarker) {
        ((BindMarker) getLimit()).appendTo(builder);
      } else {
        builder.append(getLimit());
      }
    }

    if (getPerPartitionLimit() != null) {
      builder.append(" PER PARTITION LIMIT ");
      if (getPerPartitionLimit() instanceof BindMarker) {
        ((BindMarker) getPerPartitionLimit()).appendTo(builder);
      } else {
        builder.append(getPerPartitionLimit());
      }
    }

    if (allowsFiltering()) {
      builder.append(" ALLOW FILTERING");
    }

    return builder.toString();
  }
}
