package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import java.util.Arrays;
import java.util.Objects;

/** A validated expression in a single-leg findAndRerank sort. */
public sealed interface SortExpression2
    permits SortExpression2.VectorizeSe,
        SortExpression2.VectorSe,
        SortExpression2.LexicalSe,
        SortExpression2.FieldSe {
  String path();

  record VectorizeSe(String vectorizeText) implements SortExpression2 {
    public VectorizeSe {
      Objects.requireNonNull(vectorizeText, "vectorizeText must not be null");
      if (vectorizeText.isBlank()) {
        throw new IllegalArgumentException("vectorizeText must not be blank");
      }
    }

    @Override
    public String path() {
      return VECTOR_EMBEDDING_TEXT_FIELD;
    }
  }

  record VectorSe(float[] vector) implements SortExpression2 {
    public VectorSe {
      vector = Objects.requireNonNull(vector, "vector must not be null").clone();
    }

    @Override
    public float[] vector() {
      return vector.clone();
    }

    @Override
    public String path() {
      return VECTOR_EMBEDDING_FIELD;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof VectorSe other && Arrays.equals(vector, other.vector);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(vector);
    }
  }

  record LexicalSe(String lexicalText) implements SortExpression2 {
    public LexicalSe {
      Objects.requireNonNull(lexicalText, "lexicalText must not be null");
    }

    @Override
    public String path() {
      return LEXICAL_CONTENT_FIELD;
    }
  }

  record FieldSe(String path, boolean ascending) implements SortExpression2 {
    public FieldSe {
      Objects.requireNonNull(path, "path must not be null");
    }
  }
}
