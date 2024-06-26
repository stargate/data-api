package io.stargate.sgv2.jsonapi.service.resolver.model;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.IndexUsage;

/**
 * Resolver looks at a valid {@link Command} and determines the best {@link Operation} to implement
 * the command, creates and configures it. This is the behavior of the command (how we will read or
 * insert the data) separated from the data in the Command object.
 *
 * <p>There is one resolver per Command subtype, e.g. FindOneCommandResolver may result in a
 * FindByIdOperation if the filter is just on "_id" or a FindByOneStringOperation if selecting by
 * username.
 *
 * <p>This behavior is in the resolver because we want the Commands to be dumb POJO that simply
 * represent an intention to do something, not how to do it. We can then store, replay etc commands.
 *
 * <p>It is possible for there to be multiple ways to resolve the query, the resolver should then
 * pick the one with the lowest "cost". e.g. if we can do it with a pushdown query do that. This is
 * still to be defined.
 *
 * <p><b>NOTE:</b> AS we add more C* database capabilities, such as better SAI, we create / update
 * Operations to use them and then update Resolvers so the Commands use the new DB Operations.
 *
 * @param <C> - The subtype of Command this resolver works with
 */
public interface CommandResolver<C extends Command> {

  /**
   * @return Returns class of the command the resolver is able to process.
   */
  Class<C> getCommandClass();

  /**
   * Implementations should return a {@link Operation} to execute the command that has the {@link
   * CommandContext} so it knows the db and other contextual info, and as well has all the data it
   * needs.
   *
   * @param ctx {@link CommandContext}
   * @param command {@link Command}
   * @return Operation, must no be <code>null</code>
   */
  Operation resolveCommand(CommandContext ctx, C command);

  static final String UNKNOWN_VALUE = "unknown";
  static final String TENANT_TAG = "tenant";

  /** Added count metrics for index column usage */
  default void addToMetrics(
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      Command command,
      LogicalExpression logicalExpression,
      boolean annSort) {

    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), command.getClass().getSimpleName());
    String tenant = dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE);
    Tag tenantTag = Tag.of("tenant", tenant);
    Tags tags = Tags.of(commandTag, tenantTag);

    IndexUsage indexUsage = new IndexUsage();
    if (annSort) indexUsage.vectorIndexTag = true;
    getIndexUsageTags(logicalExpression, indexUsage);
    tags = tags.and(indexUsage.getTags());

    meterRegistry.counter(jsonApiMetricsConfig.indexUsageCounterMetrics(), tags).increment();
  }

  private void getIndexUsageTags(LogicalExpression logicalExpression, IndexUsage indexUsage) {
    for (ComparisonExpression comparisonExpression : logicalExpression.comparisonExpressions) {
      getIndexUsageTags(comparisonExpression, indexUsage);
    }
    for (LogicalExpression innerLogicalExpression : logicalExpression.logicalExpressions) {
      getIndexUsageTags(innerLogicalExpression, indexUsage);
    }
  }

  private void getIndexUsageTags(ComparisonExpression comparisonExpression, IndexUsage indexUsage) {
    for (DBFilterBase dbFilterBase : comparisonExpression.getDbFilters()) {
      indexUsage.merge(dbFilterBase.indexUsage);
    }
  }
}
