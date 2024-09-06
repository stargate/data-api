package io.stargate.sgv2.jsonapi.service.resolver;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import java.util.Objects;

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
   * Call to resolve the {@link Command} into an {@link Operation} that will implement the command
   * agains the database.
   *
   * <p>Call this method, not the schema object specific ones, they are helpers for implementors so
   * they don't have to cast.
   *
   * @param commandContext Context the command is running in
   * @param command The command to resolve into an opertion
   * @return Operation, must not be <code>null</code>
   * @param <T> The type of the schema object the command is operating on.
   */
  default <T extends SchemaObject> Operation resolveCommand(
      CommandContext<T> commandContext, C command) {
    Objects.requireNonNull(commandContext, "commandContext must not be null");
    Objects.requireNonNull(command, "command must not be null");

    return switch (commandContext.schemaObject().type) {
      case COLLECTION -> resolveCollectionCommand(commandContext.asCollectionContext(), command);
      case TABLE -> resolveTableCommand(commandContext.asTableContext(), command);
      case KEYSPACE -> resolveKeyspaceCommand(commandContext.asKeyspaceContext(), command);
      case DATABASE -> resolveDatabaseCommand(commandContext.asDatabaseContext(), command);
    };
  }

  /**
   * Implementors should use this method when they can resolve commands for a collection.
   *
   * @param ctx
   * @param command
   * @return
   */
  default Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, C command) {
    // throw error as a fallback to make sure method is implemented, commands are tested well
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "%s Command does not support operating on Collections, target was %s",
        command.getClass().getSimpleName(), ctx.schemaObject().name);
  }
  ;

  /**
   * Implementors should use this method when they can resolve commands for a table.
   *
   * @param ctx
   * @param command
   * @return
   */
  default Operation resolveTableCommand(CommandContext<TableSchemaObject> ctx, C command) {
    // throw error as a fallback to make sure method is implemented, commands are tested well
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "%s Command does not support operating on Tables, target was %s",
        command.getClass().getSimpleName(), ctx.schemaObject().name);
  }

  /**
   * Implementors should use this method when they can resolve commands for a keyspace.
   *
   * @param ctx
   * @param command
   * @return
   */
  default Operation resolveKeyspaceCommand(CommandContext<KeyspaceSchemaObject> ctx, C command) {
    // throw error as a fallback to make sure method is implemented, commands are tested well
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "%s Command does not support operating on Keyspaces, target was %s",
        command.getClass().getSimpleName(), ctx.schemaObject().name);
  }

  /**
   * Implementors should use this method when they can resolve commands for a databse.
   *
   * @param ctx
   * @param command
   * @return
   */
  default Operation resolveDatabaseCommand(CommandContext<DatabaseSchemaObject> ctx, C command) {
    // throw error as a fallback to make sure method is implemented, commands are tested well
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "%s Command does not support operating on Databases, target was %s",
        command.getClass().getSimpleName(), ctx.schemaObject().name);
  }

  static final String UNKNOWN_VALUE = "unknown";
  static final String TENANT_TAG = "tenant";

  /**
   * Call to track metrics for the index usage, this method is called after the command is resolved
   * and we know the filters we want to run.
   *
   * @param meterRegistry
   * @param dataApiRequestInfo
   * @param jsonApiMetricsConfig
   * @param command
   * @param logicalExpression
   * @param baseIndexUsage Callers should pass an initial {@link IndexUsage} object that will be
   *     merged with those from the {@link DBFilterBase} used in the {@link LogicalExpression}. This
   *     means the caller can set some things the filter may not have, like using ANN in a sort. Use
   *     {@link SchemaObject#newIndexUsage()} to get the correct type of IndexUsage.
   */
  default void addToMetrics(
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      Command command,
      LogicalExpression logicalExpression,
      IndexUsage baseIndexUsage) {
    // TODO: this functions hould not be on the CommandResolver interface, it has nothing to do with
    // that
    // it's only here because of the use of records and interfaces, move to a base class
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), command.getClass().getSimpleName());
    Tag tenantTag = Tag.of(TENANT_TAG, dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    Tags tags = Tags.of(commandTag, tenantTag);

    getIndexUsageTags(logicalExpression, baseIndexUsage);
    tags = tags.and(baseIndexUsage.getTags());

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
