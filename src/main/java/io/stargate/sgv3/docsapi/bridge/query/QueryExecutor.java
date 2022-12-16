package io.stargate.sgv3.docsapi.bridge.query;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv3.docsapi.operations.ReadOperationPage;
import io.stargate.sgv3.docsapi.shredding.JSONPath;
import io.stargate.sgv3.docsapi.shredding.JsonType;
import io.stargate.sgv3.docsapi.shredding.ReadableShreddedDocument;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.javatuples.Pair;

@ApplicationScoped
public class QueryExecutor {

  private final QueriesConfig queriesConfig;

  private final StargateRequestInfo requestInfo;

  @Inject
  public QueryExecutor(QueriesConfig queriesConfig, StargateRequestInfo requestInfo) {
    this.queriesConfig = queriesConfig;
    this.requestInfo = requestInfo;
  }

  public Uni<ReadOperationPage> readDocument(QueryOuterClass.Query query, String pagingState) {
    Uni<QueryOuterClass.ResultSet> response = queryBridge(query, pagingState);
    return response
        .onItem()
        .transformToUni(
            rSet -> {
              int remaining = rSet.getRowsCount();
              int colCount = rSet.getColumnsCount();
              List<ReadableShreddedDocument> documents = new ArrayList<>(remaining);
              Iterator<QueryOuterClass.Row> rowIterator = rSet.getRowsList().stream().iterator();
              while (--remaining >= 0 && rowIterator.hasNext()) {
                QueryOuterClass.Row row = rowIterator.next();
                // Convert from a list of strings into the JSONPath
                List<JSONPath> docFieldOrder =
                    row.getValues(2).getCollection().getElementsList().stream()
                        .map(value -> value.getString())
                        .map(JSONPath::from)
                        .collect(Collectors.toUnmodifiableList());

                // read the map from the DB and convert into the JSONPath and Pair we need for the
                // shredded
                // doc.
                QueryOuterClass.Collection coll = row.getValues(3).getCollection();
                int mapSize = verifyMapLength(coll);
                Map<JSONPath, Pair<JsonType, ByteBuffer>> docAtomicFields = new HashMap<>();
                for (int i = 0; i < mapSize; i += 2) {
                  docAtomicFields.put(
                      JSONPath.from(coll.getElements(i).getString()),
                      getPairValue(coll.getElements(i + 1).getCollection()));
                }
                ReadableShreddedDocument document =
                    new ReadableShreddedDocument(
                        row.getValues(0).getString(), // key
                        Optional.of(Values.uuid(row.getValues(1))), // tx_id
                        docFieldOrder,
                        docAtomicFields);
                documents.add(document);
              }

              return Uni.createFrom()
                  .item(ReadOperationPage.from(documents, extractPagingStateFromResultSet(rSet)));
            });
  }

  private Pair<JsonType, ByteBuffer> getPairValue(QueryOuterClass.Collection collection) {
    return Pair.with(
        JsonType.fromValue((int) collection.getElements(0).getInt()),
        collection.getElements(1).getBytes().asReadOnlyByteBuffer());
  }

  private int verifyMapLength(QueryOuterClass.Collection mapValue) {
    int len = mapValue.getElementsCount();
    if ((len & 1) != 0) {
      throw new IllegalArgumentException(
          "Illegal Map representation, odd number of Value elements (" + len + ")");
    }
    return len;
  }

  protected static String extractPagingStateFromResultSet(QueryOuterClass.ResultSet rs) {
    BytesValue pagingStateOut = rs.getPagingState();
    if (pagingStateOut.isInitialized()) {
      ByteString rawPS = pagingStateOut.getValue();
      if (!rawPS.isEmpty()) {
        byte[] b = rawPS.toByteArray();
        // Could almost use "ByteBufferUtils.toBase64" but need variant that takes 'byte[]'
        return Base64.getEncoder().encodeToString(b);
      }
    }
    return null;
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

    QueryState state = ImmutableQueryState.of(100, pagingStateValue);
    QueryOuterClass.Consistency consistency = queriesConfig.consistency().reads();
    QueryOuterClass.ConsistencyValue.Builder consistencyValue =
        QueryOuterClass.ConsistencyValue.newBuilder().setValue(consistency);
    QueryOuterClass.QueryParameters.Builder params =
        QueryOuterClass.QueryParameters.newBuilder()
            .setConsistency(consistencyValue)
            .setPageSize(Int32Value.of(state.pageSize()));

    // if we have paging state, set
    if (null != state.pagingState()) {
      params.setPagingState(state.pagingState());
    }

    // final query is same as the original, just with different params
    QueryOuterClass.Query finalQuery =
        QueryOuterClass.Query.newBuilder(query).setParameters(params).buildPartial();

    // execute
    return requestInfo
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
