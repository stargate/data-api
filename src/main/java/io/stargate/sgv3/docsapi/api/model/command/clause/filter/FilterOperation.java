package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.util.EnumSet;

public interface FilterOperation {
  public boolean match(EnumSet operator, JsonType type);

  public FilterOperator operator();

  public JsonLiteral operand();
}
