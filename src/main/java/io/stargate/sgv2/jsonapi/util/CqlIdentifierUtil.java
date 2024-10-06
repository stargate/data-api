package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.util.Strings;

public abstract class CqlIdentifierUtil {

  public static CqlIdentifier cqlIdentifierFromUserInput(String name) {
    if (Strings.isDoubleQuoted(name)) {
      return CqlIdentifier.fromCql(name);
    }
    return CqlIdentifier.fromCql(Strings.doubleQuote(name));
  }
}
