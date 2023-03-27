package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.SetOperation;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** Operation that returns the documents or its key based on the filter condition. */
public record FindOperation(
    CommandContext commandContext,
    List<DBFilterBase> filters,
    /**
     * Projection used on input document; if no changes desired, identity projection. Only used for
     * "pure" read operations: for updates (like {@code findOneAndUpdate}) is passed differently.
     */
    DocumentProjector inputProjection,
    String pagingState,
    int limit,
    int pageSize,
    ReadType readType,
    ObjectMapper objectMapper)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    // get FindResponse
    return getDocuments(queryExecutor, pagingState(), null)

        // map the response to result
        .map(docs -> new ReadOperationPage(docs.docs(), docs.pagingState()));
  }

  /**
   * A operation method which can return FindResponse instead of CommandResult. This method will be
   * used by other commands which needs a document to be read.
   *
   * @param queryExecutor
   * @param pagingState
   * @param additionalIdFilter Used if a additional id filter need to be added to already available
   *     filters
   * @return
   */
  public Uni<FindResponse> getDocuments(
      QueryExecutor queryExecutor, String pagingState, DBFilterBase.IDFilter additionalIdFilter) {

    // ensure we pass failure down if read type is not DOCUMENT or KEY
    // COUNT is not supported
    switch (readType) {
      case DOCUMENT, KEY -> {
        QueryOuterClass.Query query = buildSelectQuery(additionalIdFilter);
        return findDocument(
            queryExecutor,
            query,
            pagingState,
            pageSize,
            ReadType.DOCUMENT == readType,
            objectMapper,
            inputProjection);
      }
      default -> {
        JsonApiException failure =
            new JsonApiException(
                ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported find operation read type " + readType);
        return Uni.createFrom().failure(failure);
      }
    }
  }

  /**
   * A operation method which can return ReadDocument with an empty document, if the filter
   * condition has _id filter it will return document with this field added
   *
   * @return
   */
  public ReadDocument getNewDocument() {
    ObjectNode rootNode = objectMapper().createObjectNode();
    DocumentId documentId = null;
    for (DBFilterBase filter : filters) {
      if (filter instanceof DBFilterBase.IDFilter idFilter) {
        documentId = idFilter.value;
        rootNode.putIfAbsent(filter.getPath(), filter.asJson(objectMapper().getNodeFactory()));
      } else {
        if (filter.canAddField()) {
          JsonNode value = filter.asJson(objectMapper().getNodeFactory());
          if (value != null) {
            String filterPath = filter.getPath();
            SetOperation.constructSet(filterPath, value).updateDocument(rootNode);
          }
        }
      }
    }

    return new ReadDocument(documentId, null, rootNode);
  }

  // builds select query
  private QueryOuterClass.Query buildSelectQuery(DBFilterBase.IDFilter additionalIdFilter) {
    List<BuiltCondition> conditions = new ArrayList<>(filters.size());

    // if we have id filter overwrite ignore existing IDFilter
    boolean idFilterOverwrite = additionalIdFilter != null;
    for (DBFilterBase filter : filters) {
      if (!(idFilterOverwrite && filter instanceof DBFilterBase.IDFilter)) {
        conditions.add(filter.get());
      }
    }

    // then add id overwrite if there
    if (idFilterOverwrite) {
      conditions.add(additionalIdFilter.get());
    }

    // create query
    return new QueryBuilder()
        .select()
        .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
        .from(commandContext.namespace(), commandContext.collection())
        .where(conditions)
        .limit(limit)
        .build();
  }
}
