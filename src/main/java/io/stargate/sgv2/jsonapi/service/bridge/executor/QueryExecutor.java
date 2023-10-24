package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.QueriesConfig;
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
  private final QueriesConfig queriesConfig;

  private final StargateRequestInfo stargateRequestInfo;
  /** CQLSession cache. */
  @Inject CQLSessionCache cqlSessionCache;

  @Inject
  public QueryExecutor(QueriesConfig queriesConfig, StargateRequestInfo stargateRequestInfo) {
    this.queriesConfig = queriesConfig;
    this.stargateRequestInfo = stargateRequestInfo;
  }

  /**
   * Runs the provided read document query, Updates the query with parameters
   *
   * @param query read query to be executed
   * @param pagingState read paging state provided for subsequent pages
   * @param pageSize request page size
   * @return proto result set
   */
  public Uni<QueryOuterClass.ResultSet> executeRead(
      QueryOuterClass.Query query, Optional<String> pagingState, int pageSize) {
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().reads();
    QueryOuterClass.ConsistencyValue.Builder consistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder().setConsistency(consistencyValue);
    if (pagingState.isPresent()) {
      params.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pagingState.get()))));
    }

    params.setPageSize(Int32Value.of(pageSize));
    return queryBridge(
        QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial());
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
    if (pagingState.isPresent()) {
      simpleStatement =
          simpleStatement
              .setSerialConsistencyLevel(getConsistencyLevel(queriesConfig.consistency().reads()))
              .setPageSize(pageSize)
              .setPagingState(ByteBuffer.wrap(decodeBase64(pagingState.get())));
    }
    return Uni.createFrom()
        .completionStage(cqlSessionCache.getSession().executeAsync(simpleStatement));
  }

  /**
   * Runs the provided write document query, Updates the query with parameters
   *
   * @param query write query to be executed
   * @return proto result set
   */
  public Uni<QueryOuterClass.ResultSet> executeWrite(QueryOuterClass.Query query) {
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().writes();
    QueryOuterClass.ConsistencyValue.Builder consistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
    QueryOuterClass.Consistency serialConsistency = queriesConfig.serialConsistency();
    QueryOuterClass.ConsistencyValue.Builder serialConsistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(serialConsistency);
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder()
            .setConsistency(consistencyValue)
            .setSerialConsistency(serialConsistencyValue);
    return queryBridge(
        QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial());
  }

  /**
   * Execute write query with bound statement.
   *
   * @param statement - Bound statement with query and parameters. The table name used in the query
   *     must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeWrite(SimpleStatement statement) {
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache
                .getSession()
                .executeAsync(
                    statement
                        .setConsistencyLevel(
                            getConsistencyLevel(queriesConfig.consistency().writes()))
                        .setSerialConsistencyLevel(
                            getConsistencyLevel(queriesConfig.serialConsistency()))));
  }

  /**
   * Runs the provided schema change query like create collection, Updates the query with parameters
   *
   * @param query schema change query to be executed
   * @return proto result set
   */
  public Uni<QueryOuterClass.ResultSet> executeSchemaChange(QueryOuterClass.Query query) {
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().schemaChanges();
    QueryOuterClass.ConsistencyValue.Builder consistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder().setConsistency(consistencyValue);
    return queryBridge(
        QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial());
  }

  /**
   * Execute schema change query with bound statement.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeSchemaChange(SimpleStatement boundStatement) {
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache
                .getSession()
                .executeAsync(
                    boundStatement.setSerialConsistencyLevel(
                        getConsistencyLevel(queriesConfig.consistency().schemaChanges()))));
  }

  private Uni<QueryOuterClass.ResultSet> queryBridge(QueryOuterClass.Query query) {

    // execute
    return stargateRequestInfo
        .getStargateBridge()
        .executeQuery(query)
        .map(
            response -> {
              QueryOuterClass.ResultSet resultSet = response.getResultSet();
              return resultSet;
            })
        .onFailure()
        .invoke(
            failure -> {
              logger.error("Error on bridge ", failure);
            });
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace
   * @param collectionName
   * @return
   */
  protected Uni<Optional<Schema.CqlTable>> getSchema(String namespace, String collectionName) {
    Schema.DescribeKeyspaceQuery describeKeyspaceQuery =
        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(namespace).build();
    final Uni<Schema.CqlKeyspaceDescribe> cqlKeyspaceDescribeUni =
        stargateRequestInfo.getStargateBridge().describeKeyspace(describeKeyspaceQuery);
    return cqlKeyspaceDescribeUni
        .onItemOrFailure()
        .transformToUni(
            (cqlKeyspaceDescribe, error) -> {
              if (error != null
                  && (error instanceof StatusRuntimeException sre
                      && sre.getStatus().getCode() == Status.Code.NOT_FOUND)) {
                return Uni.createFrom()
                    .failure(
                        new RuntimeException(
                            new JsonApiException(
                                ErrorCode.NAMESPACE_DOES_NOT_EXIST,
                                "The provided namespace does not exist: " + namespace)));
              }
              Schema.CqlTable cqlTable = null;
              return Uni.createFrom()
                  .item(
                      cqlKeyspaceDescribe.getTablesList().stream()
                          .filter(table -> table.getName().equals(collectionName))
                          .findFirst());
            });
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace - namespace
   * @param collectionName - collection name
   * @return TableMetadata
   */
  protected Uni<TableMetadata> getCollectionSchema(String namespace, String collectionName) {
    Optional<KeyspaceMetadata> keyspaceMetadata;
    if ((keyspaceMetadata = cqlSessionCache.getSession().getMetadata().getKeyspace(namespace))
        .isPresent()) {
      Optional<TableMetadata> tableMetadata = keyspaceMetadata.get().getTable(collectionName);
      if (tableMetadata.isPresent()) {
        return Uni.createFrom().item(tableMetadata.get());
      }
    }
    return Uni.createFrom().nullItem();
  }

  private static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }

  /**
   * Gets the consistency level for the provided QueryOuterClass.Consistency
   *
   * @param consistency - QueryOuterClass.Consistency
   * @return ConsistencyLevel
   */
  private ConsistencyLevel getConsistencyLevel(QueryOuterClass.Consistency consistency) {
    switch (consistency) {
      case ANY -> {
        return ConsistencyLevel.ANY;
      }
      case ONE -> {
        return ConsistencyLevel.ONE;
      }
      case TWO -> {
        return ConsistencyLevel.TWO;
      }
      case THREE -> {
        return ConsistencyLevel.THREE;
      }
      case QUORUM -> {
        return ConsistencyLevel.QUORUM;
      }
      case ALL -> {
        return ConsistencyLevel.ALL;
      }
      case LOCAL_QUORUM -> {
        return ConsistencyLevel.LOCAL_QUORUM;
      }
      case EACH_QUORUM -> {
        return ConsistencyLevel.EACH_QUORUM;
      }
      case SERIAL -> {
        return ConsistencyLevel.SERIAL;
      }
      case LOCAL_SERIAL -> {
        return ConsistencyLevel.LOCAL_SERIAL;
      }
      case LOCAL_ONE -> {
        return ConsistencyLevel.LOCAL_ONE;
      }
      case UNRECOGNIZED -> {
        throw new RuntimeException("Unrecognized consistency level : " + consistency);
      }
    }
    throw new RuntimeException("Unrecognized consistency level : " + consistency);
  }
}
