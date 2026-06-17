package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.ColumnDefs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import java.util.*;
import java.util.stream.Stream;

/**
 * Builder that will create {@link com.datastax.oss.driver.api.core.metadata.schema.TableMetadata}
 * and {@link com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata} instances from the
 * {@link SuperShreddingMetadata}.
 * <p>
 *  We do not create TableMetadata or IndexMetadata directly in production code, we get that from the
 *  driver. This class is for creating them for tests to fake info from the driver, and the output of this class is
 *  ground truthed against CQL. See the {@link SuperShreddingBuilder} for more details on the testing.
 * </p>
 */
public class SuperShreddingMetadataBuilder
    extends SuperShreddingBuilder<Describable, SuperShreddingMetadataBuilder> {

  @Override
  protected SuperShreddingMetadataBuilder self() {
    return this;
  }

  @Override
  public List<SuperShreddingComponent<Describable>> buildInternal() {

    // Primary key, this is the names of the columns not their def, they also need to be
    // in allColumns to get created
    var primaryKey = ColumnDefs.toColumnMetadata(ColumnDefs.PARTITION_KEY, binding())
                    .toList();

    // get the columns, including the primary keys
    // required includes the primary keys
    var columnDefs = binding().hasAnyOptional() ?
            new ArrayList<>(ColumnDefs.REQUIRED)
            : ColumnDefs.REQUIRED;
    if (binding().isVectorDefined()) {
      columnDefs.add(ColumnDefs.QUERY_VECTOR_VALUE);
    }
    if (binding().isLexicalDefined()) {
      columnDefs.add(ColumnDefs.QUERY_LEXICAL_VALUE);
    }

    // LinkedHashMap to maintain order
    Map<CqlIdentifier, ColumnMetadata> allColumns = new LinkedHashMap<>(ColumnDefs.ALL.size());
    ColumnDefs.toColumnMetadata(columnDefs, binding())
        .forEach(col -> allColumns.put(col.getName(), col));

    // map<CqlIdentifier, IndexMetadata> needed for the TableMetadata
    Map<CqlIdentifier, IndexMetadata> indexMetadata = new LinkedHashMap<>();
    buildIndexMetadata().forEach(metadata -> indexMetadata.put(metadata.getName(), metadata));

    Map<CqlIdentifier, Object> tableOptions = new LinkedHashMap<>();
    if (comment != null && !comment.isBlank()) {
      tableOptions.put(TABLE_OPTION_COMMENT_IDENTIFIER, comment);
    }

    // Metadata classes do not take defensive copies, wrap to reduce the chance of a bug elsewhere
    // updating table metadata
    var tableMetadata =
        new DefaultTableMetadata(
            binding().keyspace(),
            binding().collection(),
            UUID.randomUUID(),
            false,
            false,
            primaryKey,
            Collections.emptyMap(), // no grouping keys
            Collections.unmodifiableMap(allColumns),
            Collections.unmodifiableMap(tableOptions),
            Collections.unmodifiableMap(indexMetadata));

    List<SuperShreddingComponent<Describable>> components = new ArrayList<>(11);
    components.add(
        new SuperShreddingComponent<>(
            binding().collection(), SuperShreddingComponentType.TABLE, tableMetadata));
    indexMetadata
        .values()
        .forEach(
            index ->
                components.add(
                    new SuperShreddingComponent<>(
                        index.getName(), SuperShreddingComponentType.INDEX, index)));
    return components;
  }

  private Stream<IndexMetadata> buildIndexMetadata() {

    return indexDefs()
            .map(indexDef -> indexDef.indexMetadata(binding()));
  }
}
