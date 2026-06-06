package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.ColumnDefs;

/**
 * Builder that will create {@link com.datastax.oss.driver.api.core.metadata.schema.TableMetadata} and
 * {@link com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata} instances for the
 * {@link SuperShreddingMetadata}.
 */
public class SuperShreddingMetadataBuilder extends SuperShreddingBuilder<Describable, SuperShreddingMetadataBuilder> {


    @Override
    protected SuperShreddingMetadataBuilder self() {
        return this;
    }

    @Override
    public List<SuperShreddingComponent<Describable>> build() {

        Map<SuperShreddingMetadata.ColumnDef, Map<String, Object>> perColumnOptions = new HashMap<>();
        // Primary key first
        var primaryKey = ColumnDefs.toColumnMetadata(keyspace, collection, ColumnDefs.PARTITION_KEY)
                .toList();

        // LinkedHashMap to maintain order
        Map<CqlIdentifier, ColumnMetadata> allColumns = new LinkedHashMap<>(ColumnDefs.ALL.size());
        primaryKey.forEach(col -> allColumns.put(col.getName(), col));

        // non primary key
        var columnDefs = anyOptional() ?
                new ArrayList<>(ColumnDefs.REQUIRED)
                :
                ColumnDefs.REQUIRED;
        if (withVector()) {
            // other vector settings go into the index created for it.
            perColumnOptions.put(ColumnDefs.QUERY_VECTOR_VALUE, Map.of("dimensions", vectorLength));
            columnDefs.add(ColumnDefs.QUERY_VECTOR_VALUE);
        }
        if (withLexical()) {
            columnDefs.add(ColumnDefs.QUERY_LEXICAL_VALUE);
        }
        ColumnDefs.toColumnMetadata(keyspace, collection, columnDefs, perColumnOptions)
                .forEach(col -> allColumns.put(col.getName(), col));


        // map<CqlIdentifier, IndexMetadata> needed for the TableMetadata
        var indexMetadata = buildIndexMetadata()
                .collect(Collectors.toMap(IndexMetadata::getName, Function.identity()));

        Map<CqlIdentifier, Object> tableOptions = new LinkedHashMap<>();
        if (comment != null && !comment.isBlank()) {
            tableOptions.put(TABLE_OPTION_COMMENT_IDENTIFIER, comment);
        }

        var tableMetadata = new DefaultTableMetadata(
                keyspace,
                collection,
                UUID.randomUUID(),
                false,
                false,
                primaryKey,
                Collections.emptyMap(), // no grouping keys
                allColumns,
                tableOptions,
                indexMetadata);

        List<SuperShreddingComponent<Describable>> components = new ArrayList<>(11);
        components.add(new SuperShreddingComponent<>(collection, SuperShreddingComponentType.TABLE, tableMetadata));
        indexMetadata.values()
                .forEach(index -> components.add(new SuperShreddingComponent<>(index.getName(), SuperShreddingComponentType.INDEX, index)));
        return components;
    }

    private Stream<IndexMetadata> buildIndexMetadata(){

        var defsAndOptions = indexDefsAndOptions();
        return SuperShreddingMetadata.IndexDefs.toIndexMetadata(keyspace, collection, defsAndOptions.indexDefs(), defsAndOptions.indexOptions())
                .stream();
    }
}
