package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class QueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
  private final OperationsConfig operationsConfig;

  private final StargateRequestInfo stargateRequestInfo;
  /** CQLSession cache. */
  @Inject CQLSessionCache cqlSessionCache;

  @Inject
  public QueryExecutor(OperationsConfig operationsConfig, StargateRequestInfo stargateRequestInfo) {
    this.operationsConfig = operationsConfig;
    this.stargateRequestInfo = stargateRequestInfo;
  }

  /**
   * Execute read query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @param pagingState - In case of pagination, the paging state needs to be passed to fetch
   *     subsequent pages
   * @param pageSize - page size
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeRead(
      SimpleStatement simpleStatement, Optional<String> pagingState, int pageSize) {
    simpleStatement =
        simpleStatement
            .setPageSize(pageSize)
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());
    if (pagingState.isPresent()) {
      simpleStatement =
          simpleStatement.setPagingState(ByteBuffer.wrap(decodeBase64(pagingState.get())));
    }
    SimpleStatement finalSimpleStatement = simpleStatement;
    return cqlSessionCache
        .getSession()
        .onItem()
        .transformToUni(
            cqlSession ->
                Uni.createFrom().completionStage(cqlSession.executeAsync(finalSimpleStatement)));
  }

  /**
   * Execute write query with bound statement.
   *
   * @param statement - Bound statement with query and parameters. The table name used in the query
   *     must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeWrite(SimpleStatement statement) {
    return cqlSessionCache
        .getSession()
        .onItem()
        .transformToUni(
            cqlSession ->
                Uni.createFrom()
                    .completionStage(
                        cqlSession.executeAsync(
                            statement
                                .setConsistencyLevel(
                                    operationsConfig.queriesConfig().consistency().writes())
                                .setSerialConsistencyLevel(
                                    operationsConfig.queriesConfig().serialConsistency()))));
  }

  /**
   * Execute schema change query with bound statement.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeSchemaChange(SimpleStatement boundStatement) {
    return cqlSessionCache
        .getSession()
        .onItem()
        .transformToUni(
            cqlSession ->
                Uni.createFrom()
                    .completionStage(
                        cqlSession.executeAsync(
                            boundStatement.setSerialConsistencyLevel(
                                operationsConfig.queriesConfig().consistency().schemaChanges()))));
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace
   * @param collectionName
   * @return
   */
  protected Uni<Optional<TableMetadata>> getSchema(String namespace, String collectionName) {
    return cqlSessionCache
        .getSession()
        .onItem()
        .transform(
            session ->
                session.getMetadata().getKeyspaces().get(CqlIdentifier.fromInternal(namespace)))
        .onItem()
        .ifNull()
        .failWith(
            new JsonApiException(
                ErrorCode.NAMESPACE_DOES_NOT_EXIST,
                "The provided namespace does not exist: " + namespace))
        .onItem()
        .transform(keyspaceMetadata -> keyspaceMetadata.getTable("\"" + collectionName + "\""));
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace - namespace
   * @param collectionName - collection name
   * @return TableMetadata
   */
  protected Uni<TableMetadata> getCollectionSchema(String namespace, String collectionName) {
    return cqlSessionCache
        .getSession()
        .onItem()
        .transform(cqlSession -> cqlSession.getMetadata().getKeyspace(namespace).orElse(null))
        .onItem()
        .ifNotNull()
        .transform(keyspaceMetadata -> keyspaceMetadata.getTable(collectionName).orElse(null));
  }

  private static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }
}
