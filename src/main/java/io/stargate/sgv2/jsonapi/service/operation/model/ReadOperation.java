package io.stargate.sgv2.jsonapi.service.operation.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.MinMaxPriorityQueue;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.BytesValues;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadDocument;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * ReadOperation interface which all find command operations will use. It also provides the
 * implementation to execute and query and parse the result set as {@link FindResponse}
 */
public interface ReadOperation extends Operation {
  String[] documentColumns = {"key", "tx_id", "doc_json"};
  String[] documentKeyColumns = {"key", "tx_id"};
  String[] sortedDataColumns = {"key", "doc_json"};
  int SORTED_DATA_COLUMNS = sortedDataColumns.length;
  List<String> sortIndexColumns =
      List.of(
          "query_text_values['%s']",
          "query_dbl_values['%s']", "query_bool_values['%s']", "query_null_values['%s']");
  int SORT_INDEX_COLUMNS_SIZE = sortIndexColumns.size();
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
      ObjectMapper objectMapper,
      DocumentProjector projection) {
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
                  JsonNode root =
                      readDocument ? objectMapper.readTree(Values.string(row.getValues(2))) : null;
                  if (root != null) {
                    projection.applyProjection(root);
                  }
                  document =
                      ReadDocument.from(
                          getDocumentId(row.getValues(0)), // key
                          Values.uuid(row.getValues(1)), // tx_id
                          root);
                } catch (JsonProcessingException e) {
                  throw new JsonApiException(ErrorCode.DOCUMENT_UNPARSEABLE);
                }
                documents.add(document);
              }
              return new FindResponse(documents, extractPagingStateFromResultSet(rSet));
            });
  }

  /**
   * This method reads upto system fixed limit
   *
   * @param queryExecutor
   * @param query
   * @param pageSize
   * @param objectMapper
   * @param comparator -
   * @param numberOfOrderByColumn - Number of order by columns
   * @param skip - Skip `skip` # of document from the sorted collection before returning the
   *     documents
   * @param limit - How many documents to return
   * @param errorLimit - Count of record on which system to error out, this will be (maximum read
   *     count for sort + 1)
   * @return
   */
  default Uni<FindResponse> findOrderDocument(
      QueryExecutor queryExecutor,
      QueryOuterClass.Query query,
      int pageSize,
      ObjectMapper objectMapper,
      Comparator<ReadDocument> comparator,
      int numberOfOrderByColumn,
      int skip,
      int limit,
      int errorLimit) {
    final AtomicInteger documentCounter = new AtomicInteger(0);
    final JsonNodeFactory nodeFactory = objectMapper.getNodeFactory();
    return Multi.createBy()
        .repeating()
        .uni(
            () -> new AtomicReference<String>(null),
            stateRef -> {
              return queryExecutor.executeRead(
                  query, Optional.ofNullable(stateRef.get()), pageSize);
            })
        // Read document while pagingState exists, limit for read is set at updateLimit +1
        .whilst(resultSet -> extractPagingStateFromResultSet(resultSet) != null)
        .onItem()
        .transformToUniAndMerge(
            resultSet -> {
              Iterator<QueryOuterClass.Row> rowIterator =
                  resultSet.getRowsList().stream().iterator();
              int remaining = resultSet.getRowsCount();
              int count = documentCounter.addAndGet(remaining);
              if (count == errorLimit) throw new JsonApiException(ErrorCode.DATASET_TOO_BIG);
              List<ReadDocument> documents = new ArrayList<>(remaining);
              while (--remaining >= 0 && rowIterator.hasNext()) {
                ReadDocument document = null;
                QueryOuterClass.Row row = rowIterator.next();
                List<JsonNode> sortValues = new ArrayList<>(numberOfOrderByColumn);
                for (int sortColumnCount = 0;
                    sortColumnCount < numberOfOrderByColumn;
                    sortColumnCount++) {
                  int columnCounter =
                      SORTED_DATA_COLUMNS + ((sortColumnCount) * SORT_INDEX_COLUMNS_SIZE);
                  QueryOuterClass.Value value = row.getValues(columnCounter);
                  if (!value.hasNull()) {
                    sortValues.add(nodeFactory.textNode(Values.string(value)));
                    continue;
                  }
                  columnCounter++;
                  value = row.getValues(columnCounter);
                  if (!value.hasNull()) {
                    sortValues.add(nodeFactory.numberNode(Values.decimal(value)));
                    continue;
                  }
                  columnCounter++;
                  value = row.getValues(columnCounter);
                  if (!value.hasNull()) {
                    sortValues.add(nodeFactory.booleanNode(Values.int_(value) == 1));
                    continue;
                  }
                  columnCounter++;
                  value = row.getValues(columnCounter);
                  if (!value.hasNull()) {
                    sortValues.add(nodeFactory.nullNode());
                    continue;
                  }

                  sortValues.add(nodeFactory.missingNode());
                }
                // Create ReadDocument with document id, grpc value for doc json and list of sort
                // values
                document =
                    ReadDocument.from(
                        getDocumentId(row.getValues(0)), // key
                        new DocJsonValue(
                            objectMapper, row.getValues(1)), // Deserialized value of doc_json
                        sortValues);
                documents.add(document);
              }
              return Uni.createFrom().item(documents);
            })
        .collect()
        .in(
            () -> MinMaxPriorityQueue.orderedBy(comparator).maximumSize(skip + limit).create(),
            (sortedData, documents) -> {
              documents.forEach(doc -> sortedData.add(doc));
            })
        .onItem()
        .transform(
            sortedData -> {
              // begin value to read from the sorted list
              int begin = skip;

              // If the begin index is >= sorted list size, return empty response
              if (begin >= sortedData.size()) return new FindResponse(List.of(), null);
              // Last index to which we need to read
              int end = Math.min(skip + limit, sortedData.size());
              // Create a sublist of the required rage

              List<ReadDocument> subList = new ArrayList<>(limit);
              int i = 0;
              while (i < end) {
                ReadDocument readDocument = sortedData.poll();
                if (i >= begin) {
                  subList.add(readDocument);
                }
                i++;
              }
              // deserialize the doc_json field
              List<ReadDocument> responseDocuments =
                  subList.stream()
                      .map(
                          readDoc ->
                              ReadDocument.from(
                                  readDoc.id(), readDoc.txnId(), readDoc.docJsonValue().get()))
                      .collect(Collectors.toList());
              return new FindResponse(responseDocuments, null);
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
  /**
   * Default implementation to run count query and parse the result set
   *
   * @param queryExecutor
   * @param query
   * @return
   */
  default Uni<CountResponse> countDocuments(
      QueryExecutor queryExecutor, QueryOuterClass.Query query) {
    return queryExecutor
        .executeRead(query, Optional.empty(), 1)
        .onItem()
        .transform(
            rSet -> {
              QueryOuterClass.Row row = rSet.getRows(0); // For count there will be only one row
              int count =
                  Values.int_(row.getValues(0)); // Count value will be the first column value
              return new CountResponse(count);
            });
  }

  record FindResponse(List<ReadDocument> docs, String pagingState) {}

  record CountResponse(int count) {}

  record DocJsonValue(ObjectMapper objectMapper, QueryOuterClass.Value docJsonValue)
      implements Supplier<JsonNode> {
    public JsonNode get() {
      try {
        return objectMapper.readTree(Values.string(docJsonValue));
      } catch (JsonProcessingException e) {
        // These are data stored in the DB so the error should never happen
        // e.printStackTrace();
        return null;
      }
    }
  }
}
