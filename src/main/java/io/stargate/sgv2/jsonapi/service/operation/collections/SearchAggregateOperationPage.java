package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Result page for the {@code searchAggregate} command.
 *
 * <p>Carries the aggregation results map as a {@code status.aggregations} entry.
 */
public record SearchAggregateOperationPage(Map<String, JsonNode> aggregations)
    implements Supplier<CommandResult> {

  @Override
  public CommandResult get() {
    return CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
        .addStatus(CommandStatus.AGGREGATIONS, aggregations)
        .build();
  }
}
