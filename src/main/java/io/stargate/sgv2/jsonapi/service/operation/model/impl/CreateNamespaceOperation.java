package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;

/**
 * Operation that creates a new Cassandra keyspace that serves as a namespace for the JSON API. Will
 * create only if `stargate.jsonapi.operations.create-default-keyspace` is enabled
 *
 * @param name Name of the namespace to create.
 * @param replicationMap A replication json, see
 *     https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlCreateKeyspace.html#Table2.Replicationstrategyclassandfactorsettings.
 */
public record CreateNamespaceOperation(boolean createKeyspace, String name, String replicationMap) {

  // simple pattern for the cql
  private static final String CREATE_KEYSPACE_CQL =
      "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH REPLICATION = %s;";

  /** {@inheritDoc} */
  public Uni<Boolean> execute(QueryExecutor queryExecutor) {
    if (createKeyspace) {
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder()
              .setCql(String.format(CREATE_KEYSPACE_CQL, name, replicationMap))
              .build();

      // execute
      return queryExecutor
          .executeSchemaChange(query)

          // if we have a result always respond positively
          .map(any -> true);
    } else {
      return Uni.createFrom().item(true);
    }
  }
}
