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
 * Operation that creates a Cassandra keyspace for the Data API.
 *
 * <p>The keyspace name is already a CQL identifier when it reaches this operation. Replication map
 * keys are still plain strings because that is what the driver accepts, so the resolver validates
 * datacenter names before creating this operation.
 */
public record CreateKeyspaceOperation(
    CqlIdentifier name, String strategy, Map<String, Integer> strategyOptions)
    implements Operation {

  private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";
  private static final int DEFAULT_REPLICATION_FACTOR = 1;

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    SimpleStatement createKeyspace = buildStatement();
    return queryExecutor
        .executeCreateSchemaChange(dataApiRequestInfo, createKeyspace)
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }

  private SimpleStatement buildStatement() {
    CreateKeyspaceStart start = SchemaBuilder.createKeyspace(name).ifNotExists();

    CreateKeyspace withReplication;
    if (NETWORK_TOPOLOGY_STRATEGY.equalsIgnoreCase(strategy)) {
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
