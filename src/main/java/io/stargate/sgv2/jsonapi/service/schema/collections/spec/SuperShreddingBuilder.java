package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDefs;

import java.util.*;

/**
 * General pattern for building a super shredding "table" into different output formats.
 *
 * <p>
 * We have three different ways a table is represented:
 * <ul>
 *     <li><code>cql</code> the string representation of the table</li>
 *     <li>{@link TableMetadata} and {@link com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata} metadata from the driver, this is what the driver builds from the
 *     schema definition tables</li>
 *     <li>{@link com.datastax.oss.driver.api.core.cql.SimpleStatement} A list of statements
 *     that can be executed to create the table</li>
 * </ul>
 * </p>
 * @param <T>
 */
public abstract class SuperShreddingBuilder<T, U extends SuperShreddingBuilder<T, U>> {

    protected static final CqlIdentifier TABLE_OPTION_COMMENT_IDENTIFIER = CqlIdentifier.fromInternal("comment");

    protected boolean ifNotExists = true;
    protected CqlIdentifier keyspace;
    protected CqlIdentifier collection;
    protected int vectorLength = 0;
    protected String similarityFunction;
    protected String sourceModel;// 0 = no vector column
    protected String indexAnalyzer = null; // null = no lexical column
    protected String comment;

    public static SuperShreddingCQLBuilder cql() {
        return new SuperShreddingCQLBuilder();
    }

    public static SuperShreddingMetadataBuilder metadata() {
        return new SuperShreddingMetadataBuilder();
    }


    protected abstract U self();

    public U withIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
        return self();
    }

    public U withKeyspace(CqlIdentifier keyspace) {
        this.keyspace = keyspace;
        return self();
    }

    public U withCollection(CqlIdentifier collection) {
        this.collection = collection;
        return self();
    }

    public U withVector(int vectorLength, String similarityFunction, String sourceModel) {
        this.vectorLength = vectorLength;
        this.similarityFunction = similarityFunction;
        this.sourceModel = sourceModel;
        return self();
    }

    public U withLexical(String indexAnalyzer) {
        this.indexAnalyzer = indexAnalyzer;
        return self();
    }

    public U withComment(String comment) {
        this.comment = comment;
        return self();
    }

    public T buildTableOnly(){
        return build().stream()
                .filter(c -> c.type() == SuperShreddingComponentType.TABLE)
                .map(SuperShreddingComponent::value)
                .findFirst()
                .orElse(null);
    }

    public abstract List<SuperShreddingComponent<T>> build();

    public enum SuperShreddingComponentType{
        TABLE,
        INDEX
    }

    protected boolean withVector() {
        return vectorLength > 0;
    }

    protected boolean withLexical() {
        return indexAnalyzer != null;
    }

    protected boolean anyOptional() {
        return withVector() || withLexical();
    }

    protected record IndexDefsAndOptions(List<SuperShreddingMetadata.IndexDef> indexDefs,
                                        Map<SuperShreddingMetadata.IndexDef, Map<String, String>> indexOptions){
        protected IndexDefsAndOptions{
            indexDefs = indexDefs == null ? Collections.emptyList() : Collections.unmodifiableList(indexDefs);
            indexOptions = indexOptions == null ? Collections.emptyMap() : Collections.unmodifiableMap(indexOptions);
        }
    }

    protected IndexDefsAndOptions indexDefsAndOptions(){

        var indexDefs =  anyOptional() ?
                new ArrayList<>(IndexDefs.REQUIRED)
                :
                IndexDefs.REQUIRED;

        // NOTE: preserve order with LinkedHashMap in all placces even if not needed everywhere
        // this is important when testing against generated CQL, so do in all places
        Map<SuperShreddingMetadata.IndexDef, Map<String, String>> indexOptions = new LinkedHashMap<>();
        if (withVector()) {
            indexDefs.add(IndexDefs.QUERY_VECTOR_VALUE);
            IndexDef.vectorIndexOptions(similarityFunction, sourceModel)
                    .map(opt -> indexOptions.put(SuperShreddingMetadata.IndexDefs.QUERY_VECTOR_VALUE, opt));
        }

        if (withLexical()) {
            indexDefs.add(SuperShreddingMetadata.IndexDefs.QUERY_LEXICAL_VALUE);
            IndexDef.lexicalIndexOptions(indexAnalyzer)
                    .map(opt -> indexOptions.put(SuperShreddingMetadata.IndexDefs.QUERY_LEXICAL_VALUE, opt));
        }

        return new IndexDefsAndOptions(indexDefs, indexOptions);
    }

    public record SuperShreddingComponent<T>(CqlIdentifier identifier, SuperShreddingComponentType type, T value){

        public String asCql(){
            var cql =  switch (value){
                case Describable d -> d.describe(false);
                case String s -> s;
                default -> throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
            };
            // there is a small bug in the river IndexMetadata where it does not append ";" for a
            // CUSTOM INDEX, just check so they are all the same.
            return cql.endsWith(";") ? cql : cql + ";";
        }
    }

}
