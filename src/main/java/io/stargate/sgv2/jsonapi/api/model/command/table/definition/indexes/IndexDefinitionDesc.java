package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

/**
 * The definition of an index used in creation and listing, this does not include the index name
 * because the create index commands have the "name" and "definition" top level fields.
 */
public interface IndexDefinitionDesc<IndexColumn, IndexOption> {

  IndexColumn column();

  IndexOption options();
}
