package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.ReadDocument;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.ReadOperationPage;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.NotImplementedException;

public class FindTableOperation extends TableReadOperation {

  private final FindTableParams params;

  public FindTableOperation(
      CommandContext<TableSchemaObject> commandContext,
      LogicalExpression logicalExpression,
      FindTableParams params) {
    super(commandContext, logicalExpression);

    Preconditions.checkNotNull(params, "Params must not be null");
    this.params = params;
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    var sql =
        "select JSON * from %s.%s limit %s;"
            .formatted(
                commandContext.schemaObject().name.keyspace(),
                commandContext.schemaObject().name.table(),
                params.limit());
    var statement = SimpleStatement.newInstance(sql);

    return queryExecutor
        .executeRead(dataApiRequestInfo, statement, Optional.empty(), 100)
        .onItem()
        .transform(this::toReadOperationPage);
  }

  private ReadOperationPage toReadOperationPage(AsyncResultSet resultSet) {

    var objectMapper = new ObjectMapper();

    var allDocs =
        StreamSupport.stream(resultSet.currentPage().spliterator(), false)
            .map(
                row -> {
                  try {
                    return objectMapper.readTree(row.getString("[json]"));
                  } catch (Exception e) {
                    throw new NotImplementedException("Bang " + e.getMessage());
                  }
                })
            .map(
                jsonNode ->
                    ReadDocument.from(DocumentId.fromString("fake"), UUID.randomUUID(), jsonNode))
            .toList();

    return new ReadOperationPage(allDocs, null, params.isSingleResponse(), false, null);
  }

  public record FindTableParams(int limit) {

    public FindTableParams(int limit) {
      Preconditions.checkArgument(limit > 0, "Limit must be greater than 0");
      this.limit = limit;
    }

    public boolean isSingleResponse() {
      return limit == 1;
    }
  }
}
