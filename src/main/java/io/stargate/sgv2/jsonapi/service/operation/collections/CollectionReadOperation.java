package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.collect.MinMaxPriorityQueue;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * ReadOperation interface which all find command operations will use. It also provides the
 * implementation to execute and query and parse the result set as {@link FindResponse}
 */
public interface CollectionReadOperation extends CollectionOperation {
  String[] documentColumns = {"key", "tx_id", "doc_json"};
  String[] documentKeyColumns = {"key", "tx_id"};
  String[] sortedDataColumns = {"key", "tx_id", "doc_json"};
  int SORTED_DATA_COLUMNS = sortedDataColumns.length;
  List<String> sortIndexColumns =
      List.of(
          "query_text_values['%s']",
          "query_dbl_values['%s']",
          "query_bool_values['%s']",
          "query_null_values['%s']",
          "query_timestamp_values['%s']");
  int SORT_INDEX_COLUMNS_SIZE = sortIndexColumns.size();

  double TOTAL_TOKEN_RANGE = Math.pow(2, 64);
  double MAX_TOKEN = Math.pow(2, 63) - 1;
  double MIN_TOKEN = Math.pow(-2, 63);

  // TODO: the default implementations in here are only used on single classes, they should not be
  // here

  /**
   * Default implementation to query and parse the result set
   *
   * @param queryExecutor
   * @param queries - Multiple queries only in case of `in` condition on `_id` field
   * @param pageState
   * @param readDocument This flag is set to false if the read is done to just identify the document
   *     id and tx_id to perform another DML operation
   * @param objectMapper
   * @param projection
   * @param limit - How many documents to return
   * @param vectorSearch - whether the query uses vector search
   * @param commandName - The command that calls ReadOperation
   * @param jsonProcessingMetricsReporter - reporter to use for reporting JSON read/write metrics
   * @return
   */
  default Uni<FindResponse> findDocument(
      RequestContext dataApiRequestInfo,
      QueryExecutor queryExecutor,
      List<SimpleStatement> queries,
      String pageState,
      int pageSize,
      boolean readDocument,
      ObjectMapper objectMapper,
      DocumentProjector projection,
      int limit,
      boolean vectorSearch,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    return Multi.createFrom()
        .items(queries.stream())
        .onItem()
        .transformToUniAndMerge(
            simpleStatement -> {
              if (vectorSearch) {
                return queryExecutor.executeVectorSearch(
                    dataApiRequestInfo, simpleStatement, Optional.ofNullable(pageState), pageSize);
              } else {
                return queryExecutor.executeRead(
                    dataApiRequestInfo, simpleStatement, Optional.ofNullable(pageState), pageSize);
              }
            })
        .onItem()
        .transform(
            rSet -> {
              int remaining = rSet.remaining();
              List<ReadDocument> documents = new ArrayList<>(remaining);
              Iterator<Row> rowIterator = rSet.currentPage().iterator();
              while (--remaining >= 0 && rowIterator.hasNext()) {
                Row row = rowIterator.next();
                ReadDocument document = null;
                try {
                  // TODO: Use the field name, not the ordinal for the field this is too brittle
                  JsonNode root = readDocument ? objectMapper.readTree(row.getString(2)) : null;
                  if (root != null) {
                    // create metrics
                    // TODO Use the column names!
                    jsonProcessingMetricsReporter.reportJsonReadBytesMetrics(
                        commandName, row.getString(2).length());

                    if (projection.doIncludeSimilarityScore()) {
                      float score = row.getFloat(3); // similarity_score
                      projection.applyProjection(root, score);
                    } else {
                      projection.applyProjection(root);
                    }
                  }
                  document =
                      ReadDocument.from(
                          getDocumentId(row.getTupleValue(0)), // key
                          row.getUuid(1), // tx_id
                          root);
                } catch (JacksonException e) {
                  throw parsingExceptionToApiException(e);
                }
                documents.add(document);
              }
              return new FindResponse(documents, extractPageStateFromResultSet(rSet));
            })
        .collect()
        .asList()
        .onItem()
        .transform(list -> applyLimitToFindResponses(list, limit));
  }

  /**
   * A single findCommand may result as multiple cql queries in case of `$in` filter on `_id` field.
   *
   * <p>E.G. <code>{"find": {"filter": {"_id": {"$in": ["1", "2", "3"]}}}</code> will result as 3
   * cql queries,
   *
   * <ul>
   *   <li>SELECT key, tx_id, doc_json FROM xxx WHERE key = '1' LIMIT 2147483647
   *   <li>SELECT key, tx_id, doc_json FROM xxx WHERE key = '2' LIMIT 2147483647
   *   <li>SELECT key, tx_id, doc_json FROM xxx WHERE key = '3' LIMIT 2147483647
   * </ul>
   *
   * and then 3 findResponse. For this case, we need to merge all the responses and then apply
   * limit, since limit is applied to the command, so we need to do necessary trimming.
   *
   * <p>In terms of pageState, for above case tempPageState will be just as null, since "_id"
   * partition key select won't have pageState.
   *
   * <p>For other filter conditions, there will be just one single cql query, the pageState will be
   * set as the corresponding one. And limit has been applied to the single cql query, so the
   * trimming here won't affect anything.
   */
  private FindResponse applyLimitToFindResponses(List<FindResponse> responses, int limit) {
    List<ReadDocument> documents = new ArrayList<>();
    String tempPageState = null;
    for (FindResponse response : responses) {
      documents.addAll(response.docs());
      tempPageState = response.pageState();
    }
    if (documents.size() > limit) {
      documents = documents.subList(0, limit);
    }
    return new FindResponse(documents, tempPageState);
  }

  byte true_byte = (byte) 1;

  /**
   * This method reads upto system fixed limit
   *
   * @param queryExecutor
   * @param queries Multiple queries only in case of `in` condition on `_id` field
   * @param pageSize
   * @param objectMapper
   * @param comparator -
   * @param numberOfOrderByColumn - Number of order by columns
   * @param skip - Skip `skip` # of document from the sorted collection before returning the
   *     documents
   * @param limit - How many documents to return
   * @param errorLimit - Count of record on which system to error out, this will be (maximum read
   *     count for sort + 1)
   * @param vectorSearch - whether the query uses vector search
   * @param commandName - The command that calls ReadOperation
   * @param jsonProcessingMetricsReporter - reporter to use for reporting JSON read/write metrics
   * @return
   */
  default Uni<FindResponse> findOrderDocument(
      RequestContext dataApiRequestInfo,
      QueryExecutor queryExecutor,
      List<SimpleStatement> queries,
      int pageSize,
      ObjectMapper objectMapper,
      Comparator<ReadDocument> comparator,
      int numberOfOrderByColumn,
      int skip,
      int limit,
      int errorLimit,
      DocumentProjector projection,
      boolean vectorSearch,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    final AtomicInteger documentCounter = new AtomicInteger(0);
    final JsonNodeFactory nodeFactory = objectMapper.getNodeFactory();
    return Multi.createFrom()
        .items(queries.stream())
        .onItem()
        .transformToMultiAndMerge(
            q ->
                Multi.createBy()
                    .repeating()
                    .uni(
                        () -> new AtomicReference<String>(null),
                        stateRef -> {
                          if (vectorSearch) {
                            return queryExecutor
                                .executeVectorSearch(
                                    dataApiRequestInfo,
                                    q,
                                    Optional.ofNullable(stateRef.get()),
                                    pageSize)
                                .onItem()
                                .invoke(rs -> stateRef.set(extractPageStateFromResultSet(rs)));
                          } else {
                            return queryExecutor
                                .executeRead(
                                    dataApiRequestInfo,
                                    q,
                                    Optional.ofNullable(stateRef.get()),
                                    pageSize)
                                .onItem()
                                .invoke(rs -> stateRef.set(extractPageStateFromResultSet(rs)));
                          }
                        })
                    // Read document while pageState exists, limit for read is set at updateLimit
                    // +1
                    .whilst(resultSet -> extractPageStateFromResultSet(resultSet) != null))
        .onItem()
        .transformToUniAndMerge(
            resultSet -> {
              Iterator<Row> rowIterator = resultSet.currentPage().iterator();
              int remaining = resultSet.remaining();
              int count = documentCounter.addAndGet(remaining);
              if (count == errorLimit) {
                throw ErrorCodeV1.DATASET_TOO_BIG.toApiException(
                    "maximum sortable count = %d", errorLimit);
              }
              List<ReadDocument> documents = new ArrayList<>(remaining);
              while (--remaining >= 0 && rowIterator.hasNext()) {
                ReadDocument document = null;
                Row row = rowIterator.next();
                List<JsonNode> sortValues = new ArrayList<>(numberOfOrderByColumn);
                for (int sortColumnCount = 0;
                    sortColumnCount < numberOfOrderByColumn;
                    sortColumnCount++) {
                  int columnCounter =
                      SORTED_DATA_COLUMNS + ((sortColumnCount) * SORT_INDEX_COLUMNS_SIZE);

                  // text value
                  String value = row.getString(columnCounter);
                  if (value != null) {
                    sortValues.add(nodeFactory.textNode(value));
                    continue;
                  }
                  // number value
                  columnCounter++;
                  BigDecimal bdValue = row.getBigDecimal(columnCounter);
                  if (bdValue != null) {
                    sortValues.add(nodeFactory.numberNode(bdValue));
                    continue;
                  }
                  // boolean value
                  columnCounter++;
                  ByteBuffer boolValue = row.getBytesUnsafe(columnCounter);
                  if (boolValue != null) {
                    sortValues.add(
                        nodeFactory.booleanNode(Byte.compare(true_byte, boolValue.get(0)) == 0));
                    continue;
                  }
                  // null value
                  columnCounter++;
                  value = row.getString(columnCounter);
                  if (value != null) {
                    sortValues.add(nodeFactory.nullNode());
                    continue;
                  }
                  // date value
                  columnCounter++;
                  Instant instantValue = row.getInstant(columnCounter);
                  if (instantValue != null) {
                    sortValues.add(nodeFactory.pojoNode(new Date(instantValue.toEpochMilli())));
                    continue;
                  }
                  // missing value
                  sortValues.add(nodeFactory.missingNode());
                }
                // Create ReadDocument with document id, grpc value for doc json and list of sort
                // values
                document =
                    ReadDocument.from(
                        getDocumentId(row.getTupleValue(0)), // key
                        row.getUuid(1),
                        new DocJsonValue(
                            objectMapper, row.getString(2)), // Deserialized value of doc_json
                        sortValues);
                documents.add(document);
                jsonProcessingMetricsReporter.reportJsonReadBytesMetrics(
                    commandName, row.getString(2).length());
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
                          readDoc -> {
                            JsonNode data = readDoc.docSupplier().get();
                            projection.applyProjection(data);
                            // TODO AARON below is the old code, why do we need to create a new
                            // obj because applyProjection mutates the document ?
                            // also, if this doc was from upsert the original ReadDocument obj may
                            // not have the doc ID
                            // if there was not one in the filter.
                            // orig return ReadDocument.from(readDoc.id(), readDoc.txnId(), data);
                            return ReadDocument.from(
                                readDoc.id().orElse(null), readDoc.txnId().orElse(null), data);
                          })
                      .collect(Collectors.toList());
              return new FindResponse(responseDocuments, null);
            });
  }

  default DocumentId getDocumentId(TupleValue value) {
    int typeId = value.get(0, Byte.class);
    String documentIdAsText = value.get(1, String.class);
    return DocumentId.fromDatabase(typeId, documentIdAsText);
  }

  private String extractPageStateFromResultSet(AsyncResultSet rSet) {
    if (rSet.hasMorePages()) {
      return Base64.getEncoder().encodeToString(rSet.getExecutionInfo().getPagingState().array());
    }
    return null;
  }

  /**
   * Default implementation to run count query and parse the result set, this approach counts by key
   * field
   *
   * @param queryExecutor
   * @param simpleStatement
   * @return
   */
  default Uni<CountResponse> countDocumentsByKey(
      RequestContext dataApiRequestInfo,
      QueryExecutor queryExecutor,
      SimpleStatement simpleStatement) {
    AtomicLong counter = new AtomicLong();
    final CompletionStage<AsyncResultSet> async =
        queryExecutor
            .executeCount(dataApiRequestInfo, simpleStatement)
            .whenComplete(
                (rs, error) -> {
                  getCount(rs, error, counter);
                });

    return Uni.createFrom()
        .completionStage(async)
        .onItem()
        .transform(
            rs -> {
              return new CountResponse(counter.get());
            });
  }

  /**
   * Default implementation to run count query and parse the result set
   *
   * @param dataApiRequestInfo
   * @param queryExecutor
   * @param simpleStatement
   * @return
   */
  default Uni<CountResponse> countDocuments(
      RequestContext dataApiRequestInfo,
      QueryExecutor queryExecutor,
      SimpleStatement simpleStatement) {
    return Uni.createFrom()
        .completionStage(queryExecutor.executeCount(dataApiRequestInfo, simpleStatement))
        .onItem()
        .transform(
            rSet -> {
              Row row = rSet.one(); // For count there will be only one row
              long count = row.getLong(0); // Count value will be the first column value
              return new CountResponse(count);
            });
  }

  private void getCount(AsyncResultSet rs, Throwable error, AtomicLong counter) {
    if (error != null) {
      throw ErrorCodeV1.COUNT_READ_FAILED.toApiException();
    } else {
      counter.addAndGet(rs.remaining());
      if (rs.hasMorePages()) {
        rs.fetchNextPage().whenComplete((nextRs, e) -> getCount(nextRs, e, counter));
      }
    }
  }

  /**
   * Run estimated count query and parse the result set
   *
   * @param queryExecutor
   * @param simpleStatement
   * @return
   */
  default Uni<CountResponse> estimateDocumentCount(
      RequestContext dataApiRequestInfo,
      QueryExecutor queryExecutor,
      SimpleStatement simpleStatement) {
    AtomicLong counter = new AtomicLong();
    final CompletionStage<AsyncResultSet> async =
        queryExecutor
            .executeEstimatedCount(dataApiRequestInfo, simpleStatement)
            .whenComplete(
                (rs, error) -> {
                  getEstimatedCount(rs, error, counter);
                });

    return Uni.createFrom()
        .completionStage(async)
        .onItem()
        .transform(
            rs -> {
              return new CountResponse(counter.get());
            });
  }

  private void getEstimatedCount(AsyncResultSet rs, Throwable error, AtomicLong counter) {
    if (error != null) {
      throw ErrorCodeV1.COUNT_READ_FAILED.toApiException("root cause: %s", error.getMessage());
    } else {

      // calculate the total range size and total partitions count for each range
      double totalPartitionsCount = 0;
      double totalRangeSize = 0;

      for (Row row : rs.currentPage()) {
        long rangeStart = Long.parseLong(row.getString("range_start"));
        long rangeEnd = Long.parseLong(row.getString("range_end"));
        if (rangeStart >= rangeEnd) {
          totalRangeSize += (MAX_TOKEN - rangeStart) + (rangeEnd - MIN_TOKEN);
        } else {
          totalRangeSize += rangeEnd - rangeStart;
        }
        totalPartitionsCount += row.getLong("partitions_count");
      }

      // estimate the total row count by dividing the total partition count by the ratio
      // of the sum of all token ranges to the entire token range, avoiding division by zero
      // relies on the assumption that the supershredding schema uses one row per partition
      if (totalRangeSize > 0) {
        counter.addAndGet((long) (totalPartitionsCount / (totalRangeSize / TOTAL_TOKEN_RANGE)));
      }
    }
  }

  record FindResponse(List<ReadDocument> docs, String pageState) {}

  record CountResponse(long count) {}

  record DocJsonValue(ObjectMapper objectMapper, String docJsonValue)
      implements Supplier<JsonNode> {
    public JsonNode get() {
      try {
        return objectMapper.readTree(docJsonValue);
      } catch (JacksonException e) {
        // These are data stored in the DB so the error should never happen
        throw parsingExceptionToApiException(e);
      }
    }
  }

  /**
   * Helper method to handle details of exactly how much information to include in error message.
   */
  static JsonApiException parsingExceptionToApiException(JacksonException e) {
    return ErrorCodeV1.DOCUMENT_UNPARSEABLE.toApiException("%s", e.getOriginalMessage());
  }
}
