package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

/**
 * The definition of an index used in creation and listing. This does not include the index name
 * because the create index commands have the <code>name</code> and <code>definition</code> top
 * level fields.
 *
 * @param <IndexColumn> It is <code>String</code> for {@link VectorIndexDefinitionDesc} and {@link
 *     UnsupportedIndexDefinitionDesc} indicating column name. It is <code>RegularIndexColumn</code>
 *     for {@link RegularIndexDefinitionDesc} indicating column name and mapComponent if user wants
 *     to index on map keys/values.
 * @param <IndexOption> The class of options for the create index command.
 */
public interface IndexDefinitionDesc<IndexColumn, IndexOption> {

  IndexColumn column();

  IndexOption options();
}
