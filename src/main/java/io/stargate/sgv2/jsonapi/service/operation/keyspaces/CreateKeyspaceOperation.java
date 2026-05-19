package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Operation that creates a new Cassandra keyspace that serves as a namespace for the Data API.
 *
 * <p>The CQL statement is constructed via the driver's {@link SchemaBuilder} using a {@link
 * CqlIdentifier} for the keyspace name and the typed {@code withSimpleStrategy} / {@code
 * withNetworkTopologyStrategy} APIs for the replication map. {@code CqlIdentifier} escapes embedded
 * double quotes, removing the previous keyspace-name interpolation sink.
 *
 * <p>The driver's {@code withNetworkTopologyStrategy(Map)} does <em>not</em> escape map keys, so
 * datacenter-name map keys must still be validated by callers (see the resolver's allowlist).
 *
 * @param name Name of the keyspace to create.
 * @param strategy Replication strategy name. {@code "NetworkTopologyStrategy"} selects the
 *     network-topology strategy; any other value (including {@code null}) selects the simple
 *     strategy.
 * @param strategyOptions Strategy options. For {@code NetworkTopologyStrategy} this is the
 *     datacenter-name to replication-factor map. For the simple strategy the {@code
 *     replication_factor} entry is used (defaulting to 1). May be {@code null}.
 */
public record CreateKeyspaceOperation(
    String name, String strategy, Map<String, Integer> strategyOptions) implements Operation {

  private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";
  private static final int DEFAULT_REPLICATION_FACTOR = 1;

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    SimpleStatement createKeyspace = buildStatement();
    return queryExecutor
        .executeCreateSchemaChange(dataApiRequestInfo, createKeyspace)

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }

  SimpleStatement buildStatement() {
    CreateKeyspaceStart start =
        SchemaBuilder.createKeyspace(CqlIdentifier.fromInternal(name)).ifNotExists();

    CreateKeyspace withReplication;
    if (NETWORK_TOPOLOGY_STRATEGY.equals(strategy)) {
      withReplication =
          start.withNetworkTopologyStrategy(strategyOptions != null ? strategyOptions : Map.of());
    } else {
      int replicationFactor =
          (strategyOptions != null)
              ? strategyOptions.getOrDefault("replication_factor", DEFAULT_REPLICATION_FACTOR)
              : DEFAULT_REPLICATION_FACTOR;
      withReplication = start.withSimpleStrategy(replicationFactor);
    }
    return withReplication.build();
  }
}
