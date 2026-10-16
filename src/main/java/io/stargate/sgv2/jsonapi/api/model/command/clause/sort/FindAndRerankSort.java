package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The sort clause of a findAndRerank command, parsed by {@link
 * FindAndRerankSortClauseDeserializer}. It records the sort as the user wrote it, the {@link
 * io.stargate.sgv2.jsonapi.service.operation.reranking.FindAndRerankPlanner} decides which legs run
 * from the <code>legMode</code>, the sort values and the collection, the <code>commandFeatures
 * </code> are only for metrics.
 *
 * <p>A <code>$hybrid</code> sort is either text, <code>{"$hybrid": "cheese"}</code>, recorded in
 * <code>hybridText</code>, or an object, <code>{"$hybrid": {"$vectorize": "cheese", "$lexical":
 * "cows"}}</code>, recorded in <code>vectorizeSort</code>, <code>lexicalSort</code> and <code>
 * vectorSort</code>. Use {@link #fromHybridText(String, CommandFeatures)} and {@link
 * #fromHybridObject(String, String, float[], CommandFeatures)} to create them.
 *
 * @param legMode The kind of sort the user wrote, decided from the keys in the sort clause. The
 *     planner uses this, to decide what to run.
 * @param hybridText If <code>null</code> then the $hybrid field was not text, OR was blank text,
 *     otherwise it is the trimmed text. The planner decides which legs use it.
 * @param vectorizeSort If <code>null</code> then the $vectorize field was not present in the sort,
 *     OR was present but set to JSON null OR it was blank string, otherwise it is the value
 *     provided.
 * @param lexicalSort If <code>null</code> then the $lexical field was not present in the sort, OR
 *     was present but set to JSON null OR it was blank string, otherwise it is the value provided.
 * @param vectorSort If <code>null</code> then the vector field was not present in the sort, or
 *     present and JSON null, otherwise it is the value provided.
 * @param commandFeatures The features used by the sort clause, only used for metrics.
 */
@JsonDeserialize(using = FindAndRerankSortClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Map.class,
    examples =
        """
              {"$sort" : {"$hybrid" : "Same query for vectorize and bm25 sorting"}}}
              {"$sort" : {"$hybrid" : {"$vectorize" : "vectorize sort query" , "$lexical": "lexical sort" }}}
              {"$sort" : {"$hybrid" : {"$vector" : [1,2,3] , "$lexical": "lexical sort" }}}
      """)
public record FindAndRerankSort(
    LegMode legMode,
    String hybridText,
    String vectorizeSort,
    String lexicalSort,
    float[] vectorSort,
    CommandFeatures commandFeatures)
    implements Recordable {

  public FindAndRerankSort {
    Objects.requireNonNull(legMode, "legMode must not be null");
    Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");

    switch (legMode) {
      case FILTER -> {
        // a FILTER sort has no sort values, otherwise the planner would check them against the
        // collection and could fail with a schema error rather than
        // UNSUPPORTED_FIND_AND_RERANK_SORT
        if (hybridText != null) {
          throw new IllegalArgumentException(
              "hybridText must be null when legMode is FILTER, hybridText=" + hybridText);
        }
        if (vectorizeSort != null) {
          throw new IllegalArgumentException(
              "vectorizeSort must be null when legMode is FILTER, vectorizeSort=" + vectorizeSort);
        }
        if (lexicalSort != null) {
          throw new IllegalArgumentException(
              "lexicalSort must be null when legMode is FILTER, lexicalSort=" + lexicalSort);
        }
        if (vectorSort != null) {
          throw new IllegalArgumentException(
              "vectorSort must be null when legMode is FILTER, vectorSort.length="
                  + vectorSort.length);
        }
      }
        // a single leg sort has only the value for its mode
      case VECTORIZE -> {
        if (hybridText != null || lexicalSort != null || vectorSort != null) {
          throw new IllegalArgumentException(
              "hybridText, lexicalSort and vectorSort must be null when legMode is VECTORIZE");
        }
      }
      case VECTOR -> {
        if (hybridText != null || vectorizeSort != null || lexicalSort != null) {
          throw new IllegalArgumentException(
              "hybridText, vectorizeSort and lexicalSort must be null when legMode is VECTOR");
        }
      }
      case LEXICAL -> {
        if (hybridText != null || vectorizeSort != null || vectorSort != null) {
          throw new IllegalArgumentException(
              "hybridText, vectorizeSort and vectorSort must be null when legMode is LEXICAL");
        }
      }
      case HYBRID -> {
        // any combination is kept as written, the $hybrid text check below still applies
      }
    }

    // the $hybrid field is either text or an object, never both
    if (hybridText != null
        && (vectorizeSort != null || lexicalSort != null || vectorSort != null)) {
      throw new IllegalArgumentException(
          "vectorizeSort, lexicalSort and vectorSort must be null when hybridText is set, hybridText="
              + hybridText);
    }
  }

  /**
   * The sort for a request without a sort: the sort is <code>{}</code>, <code>null</code> or
   * missing.
   */
  public static final FindAndRerankSort NO_ARG_SORT =
      new FindAndRerankSort(LegMode.FILTER, null, null, null, null, CommandFeatures.EMPTY);

  /**
   * Creates the sort for <code>{"$hybrid": "text"}</code>.
   *
   * @param hybridText The trimmed text, <code>null</code> when the text was blank.
   * @param commandFeatures The features used by the sort clause, only used for metrics.
   */
  public static FindAndRerankSort fromHybridText(
      String hybridText, CommandFeatures commandFeatures) {
    return new FindAndRerankSort(LegMode.HYBRID, hybridText, null, null, null, commandFeatures);
  }

  /**
   * Creates the sort for <code>{"$hybrid": {"$vectorize": ..., "$lexical": ..., "$vector": ...}}
   * </code>, each value is <code>null</code> when the user did not give it a value.
   *
   * @param vectorizeSort The trimmed <code>$vectorize</code> text, or <code>null</code>.
   * @param lexicalSort The trimmed <code>$lexical</code> text, or <code>null</code>.
   * @param vectorSort The <code>$vector</code> value, or <code>null</code>.
   * @param commandFeatures The features used by the sort clause, only used for metrics.
   */
  public static FindAndRerankSort fromHybridObject(
      String vectorizeSort,
      String lexicalSort,
      float[] vectorSort,
      CommandFeatures commandFeatures) {
    return new FindAndRerankSort(
        LegMode.HYBRID, null, vectorizeSort, lexicalSort, vectorSort, commandFeatures);
  }

  /**
   * Creates the sort for <code>{"$vectorize": "text"}</code>.
   *
   * @param vectorizeSort The trimmed <code>$vectorize</code> text, or <code>null</code>.
   * @param commandFeatures The features used by the sort clause, only used for metrics.
   */
  public static FindAndRerankSort fromVectorizeSort(
      String vectorizeSort, CommandFeatures commandFeatures) {
    return new FindAndRerankSort(
        LegMode.VECTORIZE, null, vectorizeSort, null, null, commandFeatures);
  }

  /**
   * Creates the sort for <code>{"$lexical": "text"}</code>.
   *
   * @param lexicalSort The trimmed <code>$lexical</code> text, or <code>null</code>.
   * @param commandFeatures The features used by the sort clause, only used for metrics.
   */
  public static FindAndRerankSort fromLexicalSort(
      String lexicalSort, CommandFeatures commandFeatures) {
    return new FindAndRerankSort(LegMode.LEXICAL, null, null, lexicalSort, null, commandFeatures);
  }

  /**
   * Creates the sort for <code>{"$vector": [...]}</code>.
   *
   * @param vectorSort The <code>$vector</code> value, or <code>null</code>.
   * @param commandFeatures The features used by the sort clause, only used for metrics.
   */
  public static FindAndRerankSort fromVectorSort(
      float[] vectorSort, CommandFeatures commandFeatures) {
    return new FindAndRerankSort(LegMode.VECTOR, null, null, null, vectorSort, commandFeatures);
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("legMode", legMode)
        .append("hybridText", hybridText)
        .append("vectorizeSort", vectorizeSort)
        .append("lexicalSort", lexicalSort)
        .append("vectorSort", Arrays.toString(vectorSort))
        .append("commandFeatures", commandFeatures);
  }

  /**
   * Override to do a value equality check on the vector
   *
   * @param obj the reference object with which to compare.
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    return (obj instanceof FindAndRerankSort other)
        && legMode == other.legMode
        && Objects.equals(hybridText, other.hybridText)
        && Objects.equals(vectorizeSort, other.vectorizeSort)
        && Objects.equals(lexicalSort, other.lexicalSort)
        && Arrays.equals(vectorSort, other.vectorSort)
        && Objects.equals(commandFeatures, other.commandFeatures);
  }

  /** Override to do a value equality hash on the vector */
  @Override
  public int hashCode() {
    return Objects.hash(
        legMode,
        hybridText,
        vectorizeSort,
        lexicalSort,
        Arrays.hashCode(vectorSort),
        commandFeatures);
  }
}
