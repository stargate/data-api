package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import java.util.List;

public record CaptureExpression(
    Object marker, List<FilterOperation<?>> filterOperations, String path) {}
