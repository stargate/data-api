package io.stargate.sgv2.jsonapi.exception.unchecked;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.stargate.sgv2.jsonapi.util.CqlPrintUtil;

/**
 * Marks that a lightweight transaction (LWT) CQL statement failed to be applied.
 *
 * <p>This should only be used internally, we use LWT in collections because we have a
 * read-modify-write pattern for updates and deletes. This marks that CQL said the statement was not
 * applied.
 *
 * <p>Users should not be returned this exception, it should be turned into the appropriate {@link
 * io.stargate.sgv2.jsonapi.exception.APIException} before being returned to the user.
 */
public class LWTFailureException extends RuntimeException {
  public LWTFailureException(SimpleStatement statement) {
    super(
        "LWT Failed to apply: "
            + (statement == null ? "<null>" : CqlPrintUtil.trimmedCql(statement)));
  }
}
