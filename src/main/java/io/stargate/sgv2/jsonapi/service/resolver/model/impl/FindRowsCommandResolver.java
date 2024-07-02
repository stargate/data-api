package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindRowsCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindRowsCommand} */
@ApplicationScoped
public class FindRowsCommandResolver extends FilterableResolver<FindRowsCommand>
    implements CommandResolver<FindRowsCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public FindRowsCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;

    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public Class<FindRowsCommand> getCommandClass() {
    return FindRowsCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindRowsCommand command) {
    final LogicalExpression resolvedLogicalExpression = resolve(commandContext, command);

    // update if options provided
    // FindRowCommand.Options options = command.options();

    // resolve sort clause
    SortClause sortClause = command.sortClause();

    // validate sort path
    if (sortClause != null) {
      sortClause.validate(commandContext);
    }

    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        resolvedLogicalExpression,
        false);

    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // if orderBy present
    if (orderBy != null) {
      throw new IllegalStateException();
    }
    /*
    return FindOperation.unsorted(
        commandContext,
        resolvedLogicalExpression,
        command.buildProjector(),
        pageState,
        limit,
        operationsConfig.defaultPageSize(),
        ReadType.DOCUMENT,
        objectMapper,
        includeSortVector);
     */

    // !!! TO IMPLEMENT
    return null;
  }
}
