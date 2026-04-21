package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.internal.core.util.Strings;
import java.util.Comparator;

public abstract class CqlIdentifierUtil {

  public static final Comparator<CqlIdentifier> CQL_IDENTIFIER_COMPARATOR =
      Comparator.comparing(CqlIdentifier::asInternal);

  public static final Comparator<ColumnMetadata> COLUMN_METADATA_COMPARATOR =
      Comparator.comparing(m -> m.getName().asInternal());

  public static CqlIdentifier cqlIdentifierFromUserInput(String name) {
    if (Strings.isDoubleQuoted(name)) {
      return CqlIdentifier.fromCql(name);
    }
    return CqlIdentifier.fromCql(Strings.doubleQuote(name));
  }

  public static CqlIdentifier cqlIdentifierFromIndexTarget(String name) {
    return CqlIdentifier.fromInternal(name);
  }

  public static String cqlIdentifierToMessageString(CqlIdentifier identifier) {
    return identifier == null ? "null" : identifier.asCql(true);
  }

  /** Returns the API representation of a CQL identifier. */
  public static String cqlIdentifierToJsonKey(CqlIdentifier identifier) {
    return identifier.asInternal();
  }
}
