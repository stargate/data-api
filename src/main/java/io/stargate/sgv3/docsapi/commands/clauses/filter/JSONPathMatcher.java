package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.shredding.JSONPath;
import java.util.function.Predicate;

// ==================================================
// TODO Move to a better place
// ==================================================
public class JSONPathMatcher implements Predicate<JSONPath> {

  private boolean matchAll = false;
  private JSONPath matchPath;

  public JSONPathMatcher(boolean matchAll, JSONPath matchPath) {
    this.matchAll = matchAll;
    this.matchPath = matchPath;
  }

  public static Predicate<JSONPath> match(String path) {
    return ("*".equals(path))
        ? new JSONPathMatcher(true, null)
        : new JSONPathMatcher(false, JSONPath.from(path));
  }

  @Override
  public boolean test(JSONPath t) {
    return matchAll || matchPath.equals(t);
  }
}
