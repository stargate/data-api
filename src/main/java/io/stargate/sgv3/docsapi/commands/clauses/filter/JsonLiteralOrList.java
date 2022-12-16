package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.shredding.JsonType;
import java.util.List;
import java.util.function.Predicate;

// See if you can guess this one
public class JsonLiteralOrList {
  private JsonLiteral literal;
  private JsonLiteralList list;

  public JsonLiteralOrList(JsonLiteral literal, JsonLiteralList list) {
    this.literal = literal;
    this.list = list;
  }

  public static JsonLiteralOrList from(Object literal) {
    return (literal instanceof List)
        ? new JsonLiteralOrList(null, JsonLiteralList.from((List<?>) literal))
        : new JsonLiteralOrList(JsonLiteral.from(literal), null);
  }

  public JsonLiteral safeGetLiteral() {
    if (list != null) {
      throw new RuntimeException("List is not null");
    }
    return literal;
  }

  public static Predicate<JsonLiteralOrList> match(JsonType singleType) {
    return new Matcher(singleType);
  }

  private static class Matcher implements Predicate<JsonLiteralOrList> {

    // TODO: implement list etc, just testing
    public JsonType matchLiteralType;

    public Matcher(JsonType matchLiteralType) {
      this.matchLiteralType = matchLiteralType;
    }

    @Override
    public boolean test(JsonLiteralOrList t) {
      return matchLiteralType.equals(t.literal.type);
    }
  }
}
