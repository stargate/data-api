package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;

import static io.stargate.sgv2.jsonapi.util.StringUtil.isNullOrBlank;

public record SuperShreddingDef(
        CqlIdentifier keyspace,
        CqlIdentifier collection,
        boolean hasVector,
        int vectorLength,
        String similarityFunction,
        String sourceModel,
        boolean hasLexical,
        String indexAnalyzer
) {

    public boolean isVectorDefined(){
        if (!hasVector) {
            return false;
        }
        // everything should be defined
        if ( vectorLength > 0 && !isNullOrBlank(similarityFunction) && !isNullOrBlank(sourceModel)){
            return true;
        }
        // the hasVector flag was set, which can be done when we expect a vector but do not have the full spec
        // such as when we are building a predicate for ANY collection with a vector, not a specific one.
        throw new IllegalStateException("SuperShreddingDef() - hasVector is set but the vector is not defined, def=%s".formatted(this));
    }

    public boolean isLexicalDefined(){
        if (!hasLexical) {
            return false;
        }
        if (!isNullOrBlank(indexAnalyzer)) {
            return true;
        }
        // same idea as isVectorDefined()
        throw new IllegalStateException("SuperShreddingDef() - hasLexcial is set but the lexcial index is not defined, def=%s".formatted(this));
    }
    public boolean hasAnyOptional() {
        return hasVector() || hasLexical();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private CqlIdentifier keyspace;
        private CqlIdentifier collection;
        private boolean hasVector = false;
        private int vectorLength = 0;
        private String similarityFunction;
        private String sourceModel;
        private boolean hasLexical = false;
        private String indexAnalyzer = null;

        public Builder withKeyspace(CqlIdentifier keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder withCollection(CqlIdentifier collection) {
            this.collection = collection;
            return this;
        }

        public Builder withAnyVector(){
            this.hasVector = true;
            return this;
        }

        public Builder withVector(int vectorLength, String similarityFunction, String sourceModel) {
            this.vectorLength = vectorLength;
            this.similarityFunction = similarityFunction;
            this.sourceModel = sourceModel;
            this.hasVector = true;
            return this;
        }

        public Builder withAnyLexical(){
            this.hasLexical = true;
            return this;
        }

        public Builder withLexical(String indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            this.hasLexical = true;
            return this;
        }

        public SuperShreddingDef build() {
            return new SuperShreddingDef(keyspace, collection, hasVector, vectorLength, similarityFunction, sourceModel, hasLexical, indexAnalyzer);
        }
    }
}
