package io.stargate.sgv3.docsapi.commands.clauses.filter;

import java.util.List;
import java.util.stream.Collectors;

// For array comparisons.
public class JsonLiteralList {
  List<JsonLiteral> literals;

  public JsonLiteralList(List<JsonLiteral> literals) {
    this.literals = literals;
  }

  public static JsonLiteralList from(List<?> list) {
    return new JsonLiteralList(list.stream().map(JsonLiteral::from).collect(Collectors.toList()));
  }
}
