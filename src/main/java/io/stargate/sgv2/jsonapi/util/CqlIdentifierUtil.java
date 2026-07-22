package io.stargate.sgv2.jsonapi.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.internal.core.util.Strings;
import java.util.Comparator;
import java.util.Objects;

public abstract class CqlIdentifierUtil {

  public static final Comparator<CqlIdentifier> CQL_IDENTIFIER_COMPARATOR =
      Comparator.comparing(CqlIdentifier::asInternal);

  public static final Comparator<ColumnMetadata> COLUMN_METADATA_COMPARATOR =
      Comparator.comparing(m -> m.getName().asInternal());

  /**
   * Call this when we get a CQL identifier from the user input.
   *
   * <p>We are trying to preserve the original user input, including the case sensitivity. {@link
   * CqlIdentifier#fromCql(String)} will lower case the identifier if it is not quoted, and this is
   * kind of unexpected for non cql users.
   */
  public static CqlIdentifier cqlIdentifierFromUserInput(String name) {
    if (Strings.isDoubleQuoted(name)) {
      //  fromCql will see the double quotes, strip them, and make the internal retrain case.
      // e.g. `"myCol"` -> Cqlidentifer with internal set to `myCol`
      return CqlIdentifier.fromCql(name);
    }
    // the identifier does not have a double quote, so we need to double quote it to preserve case.
    // e.g. `myCol` -> `"myCol"`  -> Cqlidentifer with internal set to `myCol`
    return CqlIdentifier.fromCql(Strings.doubleQuote(name));
  }

  /**
   * See {@link #cqlIdentifierFromUserInput(String)} this will be the other side of that conversion
   *
   * @param identifier the CQL identifier to convert to CQL string
   * @return <b>NOTE:</b> The returned string will be double quoted if it needs it or not, they are
   *     wrapped without checking if they are already present.
   */
  public static String cqlIdentifierToCQL(CqlIdentifier identifier) {
    // pretty == false it means we force the double quotes around the internal without checking if
    // they are needed
    Objects.requireNonNull(identifier, "identifier must not be null");
    return identifier.asCql(false);
  }

  public static CqlIdentifier cqlIdentifierFromIndexTarget(String name) {
    return CqlIdentifier.fromInternal(name);
  }

  /**
   * Call to get the decription to use in a message like an error rmessage.
   *
   * <p>Use {@link CqlIdentifier#asInternal()} to get the internal representation which should never
   * have double quotes on it. The internal will maintain if the case if created via {@link
   * #cqlIdentifierFromUserInput(String)} because it forces double quotes, which {@link
   * CqlIdentifier#fromCql(String)} strips but leaves the case intact.
   */
  public static String cqlIdentifierToMessageString(CqlIdentifier identifier) {
    return identifier == null ? "null" : identifier.asInternal();
  }

  /** Returns the API representation of a CQL identifier. */
  public static String cqlIdentifierToJsonKey(CqlIdentifier identifier) {
    return identifier.asInternal();
  }
}
