package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import java.util.List;

/**
 * Basic struct to hold a group of tasks, their accumulator, and any Deferrable objects that were
 * used when building the tasks (and are now inner state for the tasks).
 *
 * <p>e.g. when created insert tasks, the deferrables are the {@link
 * io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow} for each insert task
 */
public record TaskGroupAndDeferrables<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>(
    TaskGroup<TaskT, SchemaT> taskGroup,
    TaskAccumulator<TaskT, SchemaT> accumulator,
    List<Deferrable> deferrables) {}
