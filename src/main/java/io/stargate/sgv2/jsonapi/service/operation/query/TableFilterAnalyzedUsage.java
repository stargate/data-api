package io.stargate.sgv2.jsonapi.service.operation.query;

import java.util.Optional;

/**
 * The analyzed usage result for {@link TableFilter}
 *
 * @param targetColumn
 * @param allowFiltering
 * @param warning
 */
public record TableFilterAnalyzedUsage(
    String targetColumn, boolean allowFiltering, Optional<String> warning) {}
