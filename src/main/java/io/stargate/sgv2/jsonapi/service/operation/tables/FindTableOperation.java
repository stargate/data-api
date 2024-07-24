package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.ReadOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.TableFilter;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: this is still a POC class, showing how we can build a filter still to do is order and
 * projections
 */
public class FindTableOperation extends TableReadOperation {

  private static final Logger LOGGER = LoggerFactory.getLogger(FindTableOperation.class);

  private final DocumentProjector projector;
  private final FindTableParams params;

  private final OperationProjection projection;

  public FindTableOperation(
      CommandContext<TableSchemaObject> commandContext,
      ObjectMapper objectMapper,
      LogicalExpression logicalExpression,
      DocumentProjector projector,
      FindTableParams params) {
    super(commandContext, logicalExpression);

    this.params = Preconditions.checkNotNull(params, "params must not be null");
    this.projector = Preconditions.checkNotNull(projector, "projector must not be null");

    projection = new AllJSONProjection(objectMapper);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // Start the select
    Select select =
        projection.forSelect(
            selectFrom(
                commandContext.schemaObject().tableMetadata.getKeyspace(),
                commandContext.schemaObject().tableMetadata.getName()));

    // BUG: this probably break order for nested expressions, for now enough to get this tested
    var tableFilters =
        logicalExpression.comparisonExpressions.stream()
            .flatMap(comparisonExpression -> comparisonExpression.getDbFilters().stream())
            .map(dbFilter -> (TableFilter) dbFilter)
            .toList();

    // Add the where clause operations
    List<Object> positionalValues = new ArrayList<>();
    for (TableFilter tableFilter : tableFilters) {
      select = tableFilter.apply(commandContext.schemaObject(), select, positionalValues);
    }

    select = select.limit(params.limit());

    // Building a statment using the positional values added by the TableFilter
    var statement = select.build(positionalValues.toArray());

    return queryExecutor
        .executeRead(dataApiRequestInfo, statement, Optional.empty(), 100)
        .onItem()
        .transform(this::toReadOperationPage);
  }

  private ReadOperationPage toReadOperationPage(AsyncResultSet resultSet) {

    var objectMapper = new ObjectMapper();

    var docSources =
        StreamSupport.stream(resultSet.currentPage().spliterator(), false)
            .map(projection::toDocument)
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
