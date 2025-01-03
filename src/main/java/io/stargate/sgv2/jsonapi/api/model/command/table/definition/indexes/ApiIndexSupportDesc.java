package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

/**
 * Describes hw an index is supported by the API for use when listing indexes.
 *
 * @param createIndex the index can be created via the API
 * @param filter the index can be used in a filter via the API
 * @param cqlDefinition the CQL definition of the index as a string
 */
public record ApiIndexSupportDesc(boolean createIndex, boolean filter, String cqlDefinition) {

  public static ApiIndexSupportDesc noSupport(String cqlDefinition) {
    return new ApiIndexSupportDesc(false, false, cqlDefinition);
  }
}
