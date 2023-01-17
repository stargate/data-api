package io.stargate.sgv3.docsapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import io.stargate.sgv3.docsapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv3.docsapi.service.operation.model.ReadOperation;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Full dynamic query generation for any of the types of filtering we can do against the the db
 * table.
 *
 * <p>Create with a series of filters that are implicitly AND'd together.
 */
public record FindOperation(
    CommandContext commandContext,
    List<DBFilterBase> filters,
    String pagingState,
    int limit,
    boolean readDocument,
    ObjectMapper objectMapper)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query = buildSelectQuery();
    return findDocument(queryExecutor, query, pagingState, readDocument, objectMapper)
        .onItem()
        .transform(docs -> new ReadOperationPage(docs.docs(), docs.pagingState()));
  }

  private QueryOuterClass.Query buildSelectQuery() {
    List<BuiltCondition> conditions = new ArrayList<>(filters.size());
    for (DBFilterBase filter : filters) {
      conditions.add(filter.get());
    }
    return new QueryBuilder()
        .select()
        .column(readDocument ? documentColumns : documentKeyColumns)
        .from(commandContext.database(), commandContext.collection())
        .where(conditions)
        .limit(limit)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FindOperation that = (FindOperation) o;
    return limit == that.limit
        && readDocument == that.readDocument
        && commandContext.equals(that.commandContext)
        && filters.equals(that.filters)
        && Objects.equals(pagingState, that.pagingState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commandContext, filters, pagingState, limit, readDocument);
  }

  /** Base for the DB filters / conditions that we want to update the dynamic query */
  public abstract static class DBFilterBase implements Supplier<BuiltCondition> {}

  /** Filter for the map columns we have in the super shredding table. */
  public abstract static class MapFilterBase<T> extends DBFilterBase {

    // NOTE: we can only do eq until SAI indexes are updated , waiting for >, < etc
    public enum Operator {
      EQ
    }

    private final String columnName;
    private final String key;
    private final Operator operator;
    private final T value;

    protected MapFilterBase(String columnName, String key, Operator operator, T value) {
      this.columnName = columnName;
      this.key = key;
      this.operator = operator;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MapFilterBase<?> that = (MapFilterBase<?>) o;
      return columnName.equals(that.columnName)
          && key.equals(that.key)
          && operator == that.operator
          && value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(columnName, key, operator, value);
    }

    @Override
    public BuiltCondition get() {
      switch (operator) {
        case EQ:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.of(key)),
              Predicate.EQ,
              getValue(value));
        default:
          throw new DocsException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported map operation %s on column %s", operator, columnName));
      }
    }
  }

  /** Filters db documents based on a text field value */
  public static class TextFilter extends MapFilterBase<String> {
    public TextFilter(String path, Operator operator, String value) {
      super("query_text_values", path, operator, value);
    }
  }

  /** Filters db documents based on a boolean field value */
  public static class BoolFilter extends MapFilterBase<Boolean> {
    public BoolFilter(String path, Operator operator, Boolean value) {
      super("query_bool_values", path, operator, value);
    }
  }

  /** Filters db documents based on a numeric field value */
  public static class NumberFilter extends MapFilterBase<BigDecimal> {
    public NumberFilter(String path, Operator operator, BigDecimal value) {
      super("query_dbl_values", path, operator, value);
    }
  }

  /** Filters db documents based on a document id field value */
  public static class IDFilter extends DBFilterBase {
    public enum Operator {
      EQ;
    }

    protected final Operator operator;
    protected final String value;

    public IDFilter(Operator operator, String value) {
      this.operator = operator;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IDFilter idFilter = (IDFilter) o;
      return operator == idFilter.operator && value.equals(idFilter.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operator, value);
    }

    @Override
    public BuiltCondition get() {
      switch (operator) {
        case EQ:
          return BuiltCondition.of(BuiltCondition.LHS.column("key"), Predicate.EQ, getValue(value));
        default:
          throw new DocsException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported id column operation %s", operator));
      }
    }
  }
  /**
   * DB filter / condition for testing a set value Note: we can only do CONTAINS until SAI indexes
   * are updated
   */
  public abstract static class SetFilterBase<T> extends DBFilterBase {
    public enum Operator {
      CONTAINS;
    }

    protected final String columnName;
    protected final T value;
    protected final Operator operator;

    protected SetFilterBase(String columnName, T value, Operator operator) {
      this.columnName = columnName;
      this.value = value;
      this.operator = operator;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetFilterBase<?> that = (SetFilterBase<?>) o;
      return columnName.equals(that.columnName)
          && value.equals(that.value)
          && operator == that.operator;
    }

    @Override
    public int hashCode() {
      return Objects.hash(columnName, value, operator);
    }

    @Override
    public BuiltCondition get() {
      switch (operator) {
        case CONTAINS:
          return BuiltCondition.of(columnName, Predicate.CONTAINS, getValue(value));
        default:
          throw new DocsException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported set operation %s on column %s", operator, columnName));
      }
    }
  }

  /**
   * Filter for document where a field == null
   *
   * <p>NOTE: cannot do != null until we get NOT CONTAINS in the DB for set
   */
  public static class IsNullFilter extends SetFilterBase<String> {
    public IsNullFilter(String path) {
      super("query_null_values", path, Operator.CONTAINS);
    }
  }

  private static QueryOuterClass.Value getValue(Object value) {
    if (value instanceof String) {
      return Values.of((String) value);
    } else if (value instanceof BigDecimal) {
      return Values.of((BigDecimal) value);
    } else if (value instanceof Boolean) {
      return Values.of((Boolean) value);
    }
    return Values.of((String) null);
  }
}
