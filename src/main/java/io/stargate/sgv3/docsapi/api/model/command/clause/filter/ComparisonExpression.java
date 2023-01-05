package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.NotEmpty;

/**
 * This object represents conditions based for a json path (node) that need to be tested Spec says
 * you can do this, compare equals to a literal {"username" : "aaron"} This is a shortcut for
 * {"username" : {"$eq" : "aaron"}} In here we expand the shortcut into a canonical long form so it
 * is all the same.
 */
public record ComparisonExpression(
    @NotNull(message = "json node path can not be null in filter") String path,
    @NotNull List<FilterOperation> filterOperations) {

  /**
   * Shortcut to create equals against a literal
   *
   * <p>e.g. {"username" : "aaron"}
   *
   * @param path Json node path
   * @param value Value returned by the deserializer
   * @return {@link ComparisonExpression} with equal operator
   */
  public static ComparisonExpression eq(String path, Object value) {
    return new ComparisonExpression(
        path, List.of(new ValueComparisonOperation(ValueComparisonOperator.EQ, getLiteral(value))));
  }

  /**
   * Create Typed JsonLiteral object for the value
   *
   * @param value object came in the request
   * @return {@link JsonLiteral}
   */
  @SuppressWarnings("rawtypes")
  private static JsonLiteral getLiteral(Object value) {
    if (value == null) {
      return new JsonLiteral<>(null, JsonType.NULL);
    }
    if (value instanceof BigDecimal) {
      return new JsonLiteral<>((BigDecimal) value, JsonType.NUMBER);
    }
    if (value instanceof Boolean) {
      return new JsonLiteral<>((Boolean) value, JsonType.BOOLEAN);
    }
    if (value instanceof String) {
      return new JsonLiteral<>((String) value, JsonType.STRING);
    }
    throw new DocsException(
        ErrorCode.FILTER_UNRESOLVABLE, String.format("Unsupported filter value type %s", value));
  }

  public List<FilterOperation> match(String matchPath, EnumSet operator, JsonType type) {
    if ("*".equals(matchPath) || matchPath.equals(path)) {
      return filterOperations.stream()
          .filter(a -> a.match(operator, type))
          .collect(Collectors.toList());
    } else {
      return List.of();
    }
  }
}
