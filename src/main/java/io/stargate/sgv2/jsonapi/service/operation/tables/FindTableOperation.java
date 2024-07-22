package io.stargate.sgv2.jsonapi.service.operation.tables;

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
import io.stargate.sgv2.jsonapi.service.operation.DocumentSource;
import io.stargate.sgv2.jsonapi.service.operation.ReadOperationPage;
import java.util.Objects;
import java.util.Optional;
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

    this.params = Objects.requireNonNull(params, "Params must not be null");
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    var cql =
        "select JSON * from %s.%s limit %s;"
            .formatted(
                commandContext.schemaObject().name.keyspace(),
                commandContext.schemaObject().name.table(),
                params.limit());
    var statement = SimpleStatement.newInstance(cql);

    return queryExecutor
        .executeRead(dataApiRequestInfo, statement, Optional.empty(), 100)
        .onItem()
        .transform(this::toReadOperationPage);
  }

  private ReadOperationPage toReadOperationPage(AsyncResultSet resultSet) {

    var objectMapper = new ObjectMapper();

    var docSources =
        StreamSupport.stream(resultSet.currentPage().spliterator(), false)
            .map(
                row ->
                    (DocumentSource)
                        () -> {
                          try {
                            return objectMapper.readTree(row.getString("[json]"));
                          } catch (Exception e) {
                            throw new NotImplementedException("Bang " + e.getMessage());
                          }
                        })
            .toList();

    return new ReadOperationPage(docSources, params.isSingleResponse(), null, false, null);
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
