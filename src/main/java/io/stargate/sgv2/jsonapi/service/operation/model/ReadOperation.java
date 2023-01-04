package io.stargate.sgv2.jsonapi.service.operation.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.bridge.grpc.BytesValues;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadDocument;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv3.docsapi.service.sequencer.QueryOptions;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequence;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ReadOperation interface which all find command operations will use. It also provides the
 * implementation to excute and query and parse the result set as {@link FindResponse}
 */
public interface ReadOperation extends Operation {
  String[] documentColumns = {"key", "tx_id", "doc_json"};
  String[] documentKeyColumns = {"key", "tx_id"};

  /** @return Read operations need an {@link ObjectMapper} for result parsing. */
  ObjectMapper objectMapper();

  /**
   * @return A operation method which can return QuerySequence with FindResponse instead of
   *     CommandResult. This method will be used by other commands which needs a document to be
   *     read.
   */
  QuerySequence<FindResponse> getDocumentsSequence();

  /**
   * Default implementation of the op sequence for query and parse the result set.
   *
   * @param query Query to execute for read.
   * @param pagingState Optional paging state
   * @param readDocument This flag is set to false if the read is done to just identify the document
   *     id and tx_id to perform another DML operation
   * @return Operation sequence that returns FindResponse.
   */
  default QuerySequence<FindResponse> findDocumentQuerySequence(
      QueryOuterClass.Query query, String pagingState, boolean readDocument) {

    return QuerySequence.query(query, QueryOptions.Type.READ)
      int pageSize,
        .withPagingState(pagingState)
        .withHandler(
            (result, throwable) -> {

              // throwable rethrow
              // TODO let's add a result only handler
              if (null != throwable) {
                throw throwable;
              }

              // process result
              int remaining = result.getRowsCount();
              List<ReadDocument> documents = new ArrayList<>(remaining);
              Iterator<QueryOuterClass.Row> rowIterator = result.getRowsList().stream().iterator();
              while (--remaining >= 0 && rowIterator.hasNext()) {
                QueryOuterClass.Row row = rowIterator.next();
                try {
                  ReadDocument document =
                      new ReadDocument(
                          getDocumentId(row.getValues(0)), // key
                          Values.uuid(row.getValues(1)), // tx_id
                          readDocument
                              ? objectMapper().readTree(Values.string(row.getValues(2)))
                              : null);
                  documents.add(document);
                } catch (JsonProcessingException e) {
                  throw new JsonApiException(ErrorCode.DOCUMENT_UNPARSEABLE);
                }
              }
              return new FindResponse(documents, extractPagingStateFromResultSet(result));
            });
  }

  /**
   * Database key type is tuple<int, text>, first field is json value type and second field is text
   *
   * @param value
   * @return
   */
  default DocumentId getDocumentId(QueryOuterClass.Value value) {
    QueryOuterClass.Collection coll = value.getCollection();
    int typeId = Values.tinyint(coll.getElements(0));
    String documentIdAsText = Values.string(coll.getElements(1));
    return DocumentId.fromDatabase(typeId, documentIdAsText);
  }

  private String extractPagingStateFromResultSet(QueryOuterClass.ResultSet rSet) {
    if (rSet.hasPagingState()) {
      return BytesValues.toBase64(rSet.getPagingState());
    }
    return null;
  }

  record FindResponse(List<ReadDocument> docs, String pagingState) {}
}
