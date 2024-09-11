package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import java.util.List;

// TIDY What does this do ?
public record CaptureExpression(
    Object marker, List<FilterOperation<?>> filterOperations, String path) {}
