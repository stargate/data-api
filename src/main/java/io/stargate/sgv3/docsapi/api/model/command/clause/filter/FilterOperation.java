package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.util.EnumSet;

/**
 * A operation represent a filter condition, currently {@link ValueComparisonOperation} is the only
 * implementation, when Array and sub document types are added it will have multiple implementation.
 */
public interface FilterOperation {
  boolean match(EnumSet<? extends FilterOperator> operator, JsonType type);

  FilterOperator operator();

  <T> JsonLiteral<T> operand();
}
