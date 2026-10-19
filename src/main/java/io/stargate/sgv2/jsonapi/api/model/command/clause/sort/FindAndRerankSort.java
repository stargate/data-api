package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The sort clause of a findAndRerank command, see {@link FindAndRerankSortClauseDeserializer} for
 * the forms it can take.
 *
 * @param rerankLegMode {@link RerankLegMode#MULTI_LEG} when the sort clause uses $hybrid, otherwise
 *     {@link RerankLegMode#SINGLE_LEG}.
 * @param hybridSort If <code>null</code> then the $hybrid field was not text, OR was blank text,
 *     otherwise it is the trimmed text.
 * @param vectorizeSort If <code>null</code> then the $vectorize field was not present in the sort
 *     clause, OR was present but set to JSON null OR it was blank string, otherwise it is the value
 *     provided.
 * @param lexicalSort If <code>null</code> then the $lexical field was not present in the sort
 *     clause, OR was present but set to JSON null OR it was blank string, otherwise it is the value
 *     provided.
 * @param vectorSort If <code>null</code> then the vector field was not present in the sort clause,
 *     or present and JSON null, otherwise it is the value provided.
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
    RerankLegMode rerankLegMode,
    String hybridSort,
    String vectorizeSort,
    String lexicalSort,
    float[] vectorSort,
    CommandFeatures commandFeatures)
    implements Recordable {

  public static final FindAndRerankSort NO_ARG_SORT =
      new FindAndRerankSort(
          RerankLegMode.SINGLE_LEG, null, null, null, null, CommandFeatures.EMPTY);

  public FindAndRerankSort {
    Objects.requireNonNull(rerankLegMode, "rerankLegMode must not be null");
    Objects.requireNonNull(commandFeatures, "commandFeatures must not be null");
  }

  /** The text to vectorize: the $hybrid text, or the $vectorize value, or <code>null</code>. */
  public String vectorizeText() {
    return hybridSort != null ? hybridSort : vectorizeSort;
  }

  /**
   * The text for the lexical sort: the $hybrid text, or the $lexical value, or <code>null</code>.
   */
  public String lexicalText() {
    return hybridSort != null ? hybridSort : lexicalSort;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("rerankLegMode", rerankLegMode)
        .append("hybridSort", hybridSort)
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
        && rerankLegMode == other.rerankLegMode
        && Objects.equals(hybridSort, other.hybridSort)
        && Objects.equals(vectorizeSort, other.vectorizeSort)
        && Objects.equals(lexicalSort, other.lexicalSort)
        && Arrays.equals(vectorSort, other.vectorSort)
        && Objects.equals(commandFeatures, other.commandFeatures);
  }

  /** Override to do a value equality hash on the vector */
  @Override
  public int hashCode() {
    return Objects.hash(
        rerankLegMode,
        hybridSort,
        vectorizeSort,
        lexicalSort,
        Arrays.hashCode(vectorSort),
        commandFeatures);
  }
}
