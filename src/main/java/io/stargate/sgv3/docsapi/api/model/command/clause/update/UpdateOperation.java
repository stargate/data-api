package io.stargate.sgv3.docsapi.api.model.command.clause.update;

public record UpdateOperation<T>(String path, UpdateOperator operator, UpdateValue<T> value) {}
