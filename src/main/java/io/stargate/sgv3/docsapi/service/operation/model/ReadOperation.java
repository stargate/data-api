package io.stargate.sgv3.docsapi.service.operation.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.BytesValues;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import io.stargate.sgv3.docsapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv3.docsapi.service.operation.model.impl.ReadDocument;
import java.util.ArrayList;
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
      int pageSize,
      boolean readDocument,
      ObjectMapper objectMapper) {
    return queryExecutor
        .executeRead(query, Optional.ofNullable(pagingState), pageSize)
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
                          Values.uuid(row.getValues(1)), // tx_id
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
    if (rSet.hasPagingState()) {
      return BytesValues.toBase64(rSet.getPagingState());
    }
    return null;
  }

  /**
   * A operation method which can return FindResponse instead of CommandResult. This method will be
   * used by other commands which needs a document to be read.
   *
   * @param queryExecutor
   * @return
   */
  Uni<FindResponse> getDocuments(QueryExecutor queryExecutor);

  public static record FindResponse(List<ReadDocument> docs, String pagingState) {}
}
