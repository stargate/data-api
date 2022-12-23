package io.stargate.sgv3.docsapi.service.bridge.executor;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import java.util.Base64;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class QueryExecutor {

  private final QueriesConfig queriesConfig;

  private final StargateRequestInfo stargateRequestInfo;

  private static final int PAGE_SIZE = 100;

  @Inject
  public QueryExecutor(QueriesConfig queriesConfig, StargateRequestInfo stargateRequestInfo) {
    this.queriesConfig = queriesConfig;
    this.stargateRequestInfo = stargateRequestInfo;
  }

  public Uni<QueryOuterClass.ResultSet> writeDocument(QueryOuterClass.Query query) {
    return queryBridge(query, null);
  }

  public Uni<QueryOuterClass.ResultSet> execute(QueryOuterClass.Query query) {
    return queryBridge(query, null);
  }

  private Uni<QueryOuterClass.ResultSet> queryBridge(
      QueryOuterClass.Query query, String pagingState) {
    // construct initial state for the query
    BytesValue pagingStateValue =
        pagingState != null ? BytesValue.of(ByteString.copyFrom(decodeBase64(pagingState))) : null;

    QueryOuterClass.Consistency consistency = queriesConfig.consistency().reads();
    QueryOuterClass.ConsistencyValue.Builder consistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder()
            .setConsistency(consistencyValue)
            .setPageSize(Int32Value.of(PAGE_SIZE));

    // if we have paging state, set
    if (null != pagingStateValue) {
      params.setPagingState(pagingStateValue);
    }

    // final query is same as the original, just with different params
    QueryOuterClass.Query finalQuery =
        QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial();

    // execute
    return stargateRequestInfo
        .getStargateBridge()
        .executeQuery(finalQuery)
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
