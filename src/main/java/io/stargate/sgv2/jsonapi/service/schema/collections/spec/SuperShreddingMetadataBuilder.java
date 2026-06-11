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
 * and {@link com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata} instances for the
 * {@link SuperShreddingMetadata}.
 */
public class SuperShreddingMetadataBuilder
    extends SuperShreddingBuilder<Describable, SuperShreddingMetadataBuilder> {

  @Override
  protected SuperShreddingMetadataBuilder self() {
    return this;
  }

  @Override
  public List<SuperShreddingComponent<Describable>> buildInternal() {

    Map<SuperShreddingMetadata.ColumnDef, Map<String, Object>> perColumnOptions = new HashMap<>();
    // Primary key first
    var primaryKey =
        ColumnDefs.toColumnMetadata(
                superShreddingDef.keyspace(),
                superShreddingDef.collection(),
                ColumnDefs.PARTITION_KEY)
            .toList();

    // LinkedHashMap to maintain order
    Map<CqlIdentifier, ColumnMetadata> allColumns = new LinkedHashMap<>(ColumnDefs.ALL.size());
    primaryKey.forEach(col -> allColumns.put(col.getName(), col));

    // non primary key
    var columnDefs =
        superShreddingDef.hasAnyOptional()
            ? new ArrayList<>(ColumnDefs.REQUIRED)
            : ColumnDefs.REQUIRED;
    if (superShreddingDef.isVectorDefined()) {
      // other vector settings go into the index created for it.
      perColumnOptions.put(
          ColumnDefs.QUERY_VECTOR_VALUE, Map.of("dimensions", superShreddingDef.vectorLength()));
      columnDefs.add(ColumnDefs.QUERY_VECTOR_VALUE);
    }
    if (superShreddingDef.isLexicalDefined()) {
      columnDefs.add(ColumnDefs.QUERY_LEXICAL_VALUE);
    }
    ColumnDefs.toColumnMetadata(
            superShreddingDef.keyspace(),
            superShreddingDef.collection(),
            columnDefs,
            perColumnOptions)
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
            superShreddingDef.keyspace(),
            superShreddingDef.collection(),
            UUID.randomUUID(),
            false,
            false,
            Collections.unmodifiableList(primaryKey),
            Collections.emptyMap(), // no grouping keys
            Collections.unmodifiableMap(allColumns),
            Collections.unmodifiableMap(tableOptions),
            Collections.unmodifiableMap(indexMetadata));

    List<SuperShreddingComponent<Describable>> components = new ArrayList<>(11);
    components.add(
        new SuperShreddingComponent<>(
            superShreddingDef.collection(), SuperShreddingComponentType.TABLE, tableMetadata));
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

    var defsAndOptions = indexDefsAndOptions(superShreddingDef);
    return SuperShreddingMetadata.IndexDefs.toIndexMetadata(
        superShreddingDef.keyspace(),
        superShreddingDef.collection(),
        defsAndOptions.indexDefs(),
        defsAndOptions.indexOptions())
        .stream();
  }
}
