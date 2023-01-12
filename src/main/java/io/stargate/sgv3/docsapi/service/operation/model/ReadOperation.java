package io.stargate.sgv3.docsapi.service.operation.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import io.stargate.sgv3.docsapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv3.docsapi.service.operation.model.impl.ReadDocument;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * ReadOperation interface which all find command operations will use. It also provides the
 * implementation to excute and query and parse the result set as {@link FindResponse}
 */
public interface ReadOperation extends Operation {
  static String[] documentColumns = {"key", "tx_id", "doc_json"};
  static String[] documentKeyColumns = {"key", "tx_id"};

  /**
   * Default implementation to query and parse the result set
   *
   * @param queryExecutor
   * @param query
   * @param pagingState
   * @param readDocument This flag is set to false if the read is done to just identify the document
   *     id and tx_id to perform another DML operation
   * @param objectMapper
   * @return
   */
  default Uni<FindResponse> findDocument(
      QueryExecutor queryExecutor,
      QueryOuterClass.Query query,
      String pagingState,
      boolean readDocument,
      ObjectMapper objectMapper) {
    return queryExecutor
        .executeRead(query, Optional.ofNullable(pagingState), Optional.empty())
        .onItem()
        .transform(
            rSet -> {
              int remaining = rSet.getRowsCount();
              int colCount = rSet.getColumnsCount();
              List<ReadDocument> documents = new ArrayList<>(remaining);
              Iterator<QueryOuterClass.Row> rowIterator = rSet.getRowsList().stream().iterator();
              while (--remaining >= 0 && rowIterator.hasNext()) {
                QueryOuterClass.Row row = rowIterator.next();
                ReadDocument document = null;
                try {
                  document =
                      new ReadDocument(
                          Values.string(row.getValues(0)), // key
                          Optional.of(Values.uuid(row.getValues(1))), // tx_id
                          readDocument
                              ? objectMapper.readTree(Values.string(row.getValues(2)))
                              : null);
                } catch (JsonProcessingException e) {
                  throw new DocsException(ErrorCode.DOCUMENT_UNPARSEABLE);
                }
                documents.add(document);
              }
              return new FindResponse(documents, extractPagingStateFromResultSet(rSet));
            });
  }

  private String extractPagingStateFromResultSet(QueryOuterClass.ResultSet rSet) {
    BytesValue pagingStateOut = rSet.getPagingState();
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

  public static record FindResponse(List<ReadDocument> docs, String pagingState) {}
}
