package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Full dynamic query generation for any of the types of filtering we can do against the the db
 * table.
 *
 * <p>Create with a series of filters that are implicitly AND'd together.
 *
 * <p>TODO : handle logic operations once we get OR in the DB
 */
public class FindDynamicQueryOperation extends ReadOperation {

  private final List<DBFilterBase> filters;
  /**
   * @param commandContext
   * @param filters The list of filters / conditions to use when finding the document, see inner
   *     classes.
   */
  private final String pagingState;

  private final int limit;

  public FindDynamicQueryOperation(
      CommandContext commandContext, List<DBFilterBase> filters, String pagingState, int limit) {
    super(commandContext);
    this.filters = filters;
    this.pagingState = pagingState;
    this.limit = limit;
  }

  @Override
  protected Uni<ReadOperationPage> executeInternal(QueryExecutor queryExecutor) {
    return queryExecutor.readDocument(selectBuilder(getCommandContext()), pagingState);
  }

  private QueryOuterClass.Query selectBuilder(CommandContext commandContext) {
    List<BuiltCondition> conditions = new ArrayList<>(filters.size());
    for (DBFilterBase filter : filters) {
      conditions.add(filter.get());
    }
    QueryOuterClass.Query select =
        new QueryBuilder()
            .select()
            .column("key", "tx_id", "doc_field_order", "doc_atomic_fields")
            .from(commandContext.database, commandContext.collection)
            .where(conditions)
            .limit(limit != 0 ? limit : Integer.MAX_VALUE)
            .build();
    return select;
  }

  @Override
  public OperationPlan getPlan() {
    return new OperationPlan(true);
  }

  /** Base for the DB filters / conditions that we want to update the dynamic query */
  public abstract static class DBFilterBase implements Supplier<BuiltCondition> {}

  /** Filter for the map columns we have in the super shredding table. */
  public abstract static class MapFilterBase<T> extends DBFilterBase {

    // NOTE: we can only do eq until SAI indexes are updated , waiting for >, < etc
    public enum Operator {
      EQ;
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
    public BuiltCondition get() {
      switch (operator) {
        case EQ:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.of(key)),
              Predicate.EQ,
              getValue(value));
        default:
          throw new RuntimeException(
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

  /** DB filter / condition for testing a set value */
  public abstract static class SetFilterBase<T> extends DBFilterBase {

    // NOTE: we can only do CONTAINS until SAI indexes are updated
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
    public BuiltCondition get() {
      switch (operator) {
        case CONTAINS:
          return BuiltCondition.of(columnName, Predicate.CONTAINS, getValue(value));
        default:
          throw new RuntimeException(
              String.format("Unsupported map operation %s on column %s", operator, columnName));
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
      super("query_null_values", path, SetFilterBase.Operator.CONTAINS);
    }
  }

  private static QueryOuterClass.Value getValue(Object value) {
    if (value instanceof String) {
      return Values.of((String) value);
    } else if (value instanceof Double) {
      return Values.of((Double) value);
    } else if (value instanceof Boolean) {
      return Values.of((Boolean) value);
    }
    throw new RuntimeException("Unknown format for value " + value);
  }
}
