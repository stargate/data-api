package io.stargate.sgv3.docsapi.service.bridge.executor;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv3.docsapi.service.bridge.config.DocumentConfig;
import java.util.Base64;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class QueryExecutor {

  private final QueriesConfig queriesConfig;

  private final DocumentConfig documentConfig;

  private final StargateRequestInfo stargateRequestInfo;

  @Inject
  public QueryExecutor(
      QueriesConfig queriesConfig,
      DocumentConfig documentConfig,
      StargateRequestInfo stargateRequestInfo) {
    this.queriesConfig = queriesConfig;
    this.documentConfig = documentConfig;
    this.stargateRequestInfo = stargateRequestInfo;
  }

  public Uni<QueryOuterClass.ResultSet> executeRead(
      QueryOuterClass.Query query, Optional<String> pagingState, Optional<Integer> pageSize) {
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().reads();
    QueryOuterClass.ConsistencyValue.Builder consistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder().setConsistency(consistencyValue);
    if (pagingState.isPresent()) {
      params.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pagingState.get()))));
    }

    if (pageSize.isPresent()) {
      params.setPageSize(Int32Value.of(pageSize.get()));
    } else {
      params.setPageSize(Int32Value.of(documentConfig.maxPageSize()));
    }
    return queryBridge(
        QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial());
  }

  public Uni<QueryOuterClass.ResultSet> executeWrite(QueryOuterClass.Query query) {
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().writes();
    QueryOuterClass.ConsistencyValue.Builder consistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder().setConsistency(consistencyValue);
    return queryBridge(
        QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial());
  }

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
              // update next state
              QueryOuterClass.ResultSet resultSet = response.getResultSet();
              return resultSet;
            });
  }

  protected static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }
}
