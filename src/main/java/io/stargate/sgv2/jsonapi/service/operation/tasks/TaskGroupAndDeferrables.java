package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import java.util.List;

public record TaskGroupAndDeferrables<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>(
    TaskGroup<TaskT, SchemaT> taskGroup,
    TaskAccumulator<TaskT, SchemaT> accumulator,
    List<Deferrable> deferrables) {}
