package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.exception.ErrorTemplate;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import java.util.Map;
import java.util.regex.Pattern;

final class KeyspaceCommandResolverSupport {

  private static final Pattern DROP_KEYSPACE_NAME = Pattern.compile("\\w+");

  private KeyspaceCommandResolverSupport() {}

  static CqlIdentifier keyspaceIdentifierForCreate(String name) {
    return cqlIdentifierFromUserInput(NamingRules.KEYSPACE.checkRule(name));
  }

  static CqlIdentifier keyspaceIdentifierForDrop(String name) {
    if (name == null || !DROP_KEYSPACE_NAME.matcher(name).matches()) {
      throw SchemaException.Code.INVALID_KEYSPACE.get(
          Map.of("keyspace", ErrorTemplate.replaceIfNull(name)));
    }
    return cqlIdentifierFromUserInput(name);
  }
}
