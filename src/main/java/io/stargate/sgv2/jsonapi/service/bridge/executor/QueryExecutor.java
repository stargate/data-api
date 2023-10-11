package io.stargate.sgv2.jsonapi.service.bridge.executor;

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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Base64;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class QueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
  private final QueriesConfig queriesConfig;

  private final StargateRequestInfo stargateRequestInfo;

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

  private static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }
}
