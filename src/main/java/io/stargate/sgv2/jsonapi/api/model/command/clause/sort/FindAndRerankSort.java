package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The sort clause of a findAndRerank command, parsed by {@link
 * FindAndRerankSortClauseDeserializer}. The {@link
 * io.stargate.sgv2.jsonapi.service.operation.reranking.FindAndRerankPlanner} decides which legs run
 * from the <code>sortMode</code>, <code>explicitLexical</code> and the sort values, the <code>
 * commandFeatures</code> are only for metrics.
 *
 * @param sortMode The kind of sort the user wrote, decided from the keys in the sort clause. The
 *     planner uses this, not the <code>commandFeatures</code>, to decide what to run.
 * @param explicitLexical <code>true</code> if the user wrote the $lexical field in the $hybrid
 *     object, even when it was set to JSON null or a blank string. When the collection has no
 *     lexical index, an explicit $lexical with a value is an error, while lexical text that came
 *     from the $hybrid string is skipped.
 * @param vectorizeSort If <code>null</code> then the $vectorize field was not present in the sort
 *     clause, OR was present but set to JSON null OR it was blank string, otherwise it is the value
 *     provided.
 * @param lexicalSort If <code>null</code> then the $lexical field was not present in the sort
 *     clause, OR was present but set to JSON null OR it was blank string, otherwise it is the value
 *     provided.
 * @param vectorSort If <code>null</code> then the vector field was not present in the sort clause,
 *     or present and JSON null, otherwise it is the value provided.
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
    SortMode sortMode,
    boolean explicitLexical,
    String vectorizeSort,
    String lexicalSort,
    float[] vectorSort,
    CommandFeatures commandFeatures)
    implements Recordable {

  public FindAndRerankSort {
    Objects.requireNonNull(sortMode, "sortMode must not be null");
    Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");

    // a FILTER sort has no sort values, otherwise the planner would check them against the
    // collection and could fail with a schema error rather than UNSUPPORTED_FIND_AND_RERANK_SORT
    if (sortMode == SortMode.FILTER) {
      if (explicitLexical) {
        throw new IllegalArgumentException("explicitLexical must be false when sortMode is FILTER");
      }
      if (vectorizeSort != null) {
        throw new IllegalArgumentException(
            "vectorizeSort must be null when sortMode is FILTER, vectorizeSort=" + vectorizeSort);
      }
      if (lexicalSort != null) {
        throw new IllegalArgumentException(
            "lexicalSort must be null when sortMode is FILTER, lexicalSort=" + lexicalSort);
      }
      if (vectorSort != null) {
        throw new IllegalArgumentException(
            "vectorSort must be null when sortMode is FILTER, vectorSort.length="
                + vectorSort.length);
      }
    }
  }

  /**
   * Creates the sort for a request without a sort: the sort is <code>{}</code>, <code>null</code>
   * or missing.
   *
   * <p>Returns a new object on every call, because the {@link CommandFeatures} are mutable and must
   * not be shared between requests.
   */
  public static FindAndRerankSort noArgSort() {
    return new FindAndRerankSort(
        SortMode.FILTER, false, null, null, null, CommandFeatures.create());
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("sortMode", sortMode)
        .append("explicitLexical", explicitLexical)
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
        && sortMode == other.sortMode
        && explicitLexical == other.explicitLexical
        && Objects.equals(vectorizeSort, other.vectorizeSort)
        && Objects.equals(lexicalSort, other.lexicalSort)
        && Arrays.equals(vectorSort, other.vectorSort)
        && Objects.equals(commandFeatures, other.commandFeatures);
  }

  /** Override to do a value equality hash on the vector */
  @Override
  public int hashCode() {
    return Objects.hash(
        sortMode,
        explicitLexical,
        vectorizeSort,
        lexicalSort,
        Arrays.hashCode(vectorSort),
        commandFeatures);
  }
}
