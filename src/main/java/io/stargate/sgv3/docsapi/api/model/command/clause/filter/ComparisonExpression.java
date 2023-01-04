package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public record ComparisonExpression(
    // the node in in the doc we want to test.
    String path,

    // Spec says you can do this, compare equals to a literal {"username" : "aaron"}
    // This is a shortcut for {"username" : {"$eq" : "aaron"}}
    // In here we expand the shortcut into a canonical long form so it is all the same.
    List<FilterOperation> filterOperations) {

  /**
   * Shortcut to create equals against a literal
   *
   * <p>e.g. {"username" : "aaron"}
   *
   * @param path
   * @param value
   * @return
   */
  public static ComparisonExpression eq(String path, Object value) {
    return new ComparisonExpression(
        path,
        List.of(
            new ValueComparisonOperation(
                ValueComparisonOperator.EQ, new JsonLiteral(value, JsonType.typeForValue(value)))));
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
