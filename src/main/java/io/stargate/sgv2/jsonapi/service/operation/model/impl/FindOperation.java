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
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTargetLocator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** Operation that returns the documents or its key based on the filter condition. */
public record FindOperation(
    CommandContext commandContext,
    List<DBFilterBase> filters,
    String pagingState,
    int limit,
    int pageSize,
    ReadType readType,
    ObjectMapper objectMapper)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    return getDocuments(queryExecutor, pagingState(), null)
        .onItem()
        .transform(docs -> new ReadOperationPage(docs.docs(), docs.pagingState()));
  }

  @Override
  public Uni<FindResponse> getDocuments(
      QueryExecutor queryExecutor, String pagingState, DBFilterBase.IDFilter additionalIdFilter) {
    switch (readType) {
      case DOCUMENT:
      case KEY:
        {
          QueryOuterClass.Query query = buildSelectQuery(additionalIdFilter);
          return findDocument(
              queryExecutor,
              query,
              pagingState,
              pageSize,
              ReadType.DOCUMENT == readType,
              objectMapper);
        }
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_OPERATION, "Unsupported find operation read type " + readType);
    }
  }

  @Override
  public ReadDocument getNewDocument() {
    ObjectNode rootNode = objectMapper().createObjectNode();
    DocumentId documentId = null;
    UpdateTargetLocator targetLocator = new UpdateTargetLocator();
    for (DBFilterBase filter : filters) {
      if (filter instanceof DBFilterBase.IDFilter) {
        documentId = ((DBFilterBase.IDFilter) filter).value;
        rootNode.putIfAbsent(filter.getPath(), filter.asJson(objectMapper().getNodeFactory()));
      } else {
        if (filter.canAddField()) {
          JsonNode value = filter.asJson(objectMapper().getNodeFactory());
          if (value != null) {
            String filterPath = filter.getPath();
            SetOperation.construct(filterPath, value).updateDocument(rootNode, targetLocator);
          }
        }
      }
    }
    ReadDocument doc = new ReadDocument(documentId, null, rootNode);
    return doc;
  }

  private QueryOuterClass.Query buildSelectQuery(DBFilterBase.IDFilter additionalIdFilter) {
    List<BuiltCondition> conditions = new ArrayList<>(filters.size());
    for (DBFilterBase filter : filters) {
      if (additionalIdFilter == null
          || (additionalIdFilter != null && !(filter instanceof DBFilterBase.IDFilter)))
        conditions.add(filter.get());
    }
    if (additionalIdFilter != null) {
      conditions.add(additionalIdFilter.get());
    }
    return new QueryBuilder()
        .select()
        .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
        .from(commandContext.namespace(), commandContext.collection())
        .where(conditions)
        .limit(limit)
        .build();
  }
}
