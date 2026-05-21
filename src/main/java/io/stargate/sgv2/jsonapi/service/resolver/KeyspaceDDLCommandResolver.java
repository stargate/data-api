package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.exception.ErrorTemplate;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class KeyspaceDDLCommandResolver<C extends Command> implements CommandResolver<C> {

  private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";

  // NetworkTopologyStrategy uses replication map option keys as data center names. Cassandra
  // validates those keys semantically against known data centers; they are not CQL identifiers.
  // The Java driver query builder renders replication map keys as single-quoted CQL string
  // literals, appending the raw key without escaping it. Because the CQL delimiter in this position
  // is the ASCII single quote, reject that character and leave non-delimiters such as double quotes,
  // backticks, and curly quotes to Cassandra's own data-center validation.
  private static final Pattern VALID_DATA_CENTER_NAME = Pattern.compile("^[^']+$");

  protected CqlIdentifier keyspaceIdentifierForCreate(String name) {
    return cqlIdentifierFromUserInput(NamingRules.KEYSPACE.checkRule(name));
  }

  protected CqlIdentifier keyspaceIdentifierForDrop(String name) {
    return cqlIdentifierFromUserInput(NamingRules.KEYSPACE.sanitizeInput(name));
  }

  protected void validateStrategyOptions(String strategy, Map<String, Integer> strategyOptions) {
    if (!isNetworkTopologyStrategy(strategy) || strategyOptions == null) {
      return;
    }
    for (String dcName : strategyOptions.keySet()) {
      checkDataCenterName(dcName);
    }
  }

  private boolean isNetworkTopologyStrategy(String strategy) {
    return NETWORK_TOPOLOGY_STRATEGY.equals(strategy);
  }

  private void checkDataCenterName(String name) {
    if (name == null || !VALID_DATA_CENTER_NAME.matcher(name).matches()) {
      throw SchemaException.Code.INVALID_REPLICATION_DATA_CENTER_NAME.get(
          Map.of("invalidDataCenterName", ErrorTemplate.replaceIfNull(name)));
    }
  }
}
