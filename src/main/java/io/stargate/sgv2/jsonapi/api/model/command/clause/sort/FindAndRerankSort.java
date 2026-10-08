package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * @param legMode The kind of retrieval the user asked for, decided from the keys in the sort
 *     clause. The resolver uses this, not the <code>commandFeatures</code>, to decide what to run.
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
    LegMode legMode,
    boolean explicitLexical,
    String vectorizeSort,
    String lexicalSort,
    float[] vectorSort,
    CommandFeatures commandFeatures)
    implements Recordable {

  public FindAndRerankSort {
    Objects.requireNonNull(legMode, "legMode must not be null");
  }

  /**
   * Creates a {@link LegMode#HYBRID} sort where the $lexical field was not explicitly set.
   *
   * @param vectorizeSort See the record docs.
   * @param lexicalSort See the record docs.
   * @param vectorSort See the record docs.
   * @param commandFeatures See the record docs.
   */
  public FindAndRerankSort(
      String vectorizeSort,
      String lexicalSort,
      float[] vectorSort,
      CommandFeatures commandFeatures) {
    this(LegMode.HYBRID, false, vectorizeSort, lexicalSort, vectorSort, commandFeatures);
  }

  /**
   * Creates the sort for a request without a sort: the sort is <code>{}</code>, <code>null</code>
   * or missing.
   *
   * <p>Returns a new object on every call, because the {@link CommandFeatures} are mutable and must
   * not be shared between requests.
   */
  public static FindAndRerankSort noArgSort() {
    return new FindAndRerankSort(LegMode.FILTER, false, null, null, null, CommandFeatures.create());
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("legMode", legMode)
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
        && legMode == other.legMode
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
        legMode,
        explicitLexical,
        vectorizeSort,
        lexicalSort,
        Arrays.hashCode(vectorSort),
        commandFeatures);
  }
}
