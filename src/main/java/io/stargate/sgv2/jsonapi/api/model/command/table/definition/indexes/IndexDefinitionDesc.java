package io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes;

/**
 * The definition of an index used in creation and listing, this does not include the index name
 * because the create index commands have the "name" and "definition" top level fields. <br>
 * IndexColumn type is different among {@link RegularIndexDefinitionDesc}, {@link
 * VectorIndexDefinitionDesc} and {@link UnsupportedIndexDefinitionDesc}.<br>
 *
 * @param <IndexColumn> It is String for VectorIndexDefinitionDesc and
 *     UnsupportedIndexDefinitionDesc indicating column name. It is RegularIndexColumn for
 *     RegularIndexDefinitionDesc indicating column name and mapComponent if user wants to index on
 *     map keys/values.
 */
public interface IndexDefinitionDesc<IndexColumn, IndexOption> {

  IndexColumn column();

  IndexOption options();
}
