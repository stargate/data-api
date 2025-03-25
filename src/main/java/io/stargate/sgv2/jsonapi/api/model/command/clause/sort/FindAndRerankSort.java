package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
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
    example =
        """
              {"$sort" : {"$hybrid" : "Same query for vectorize and bm25 sorting"}}}
              {"$sort" : {"$hybrid" : {"$vectorize" : "vectorize sort query" , "$lexical": "lexical sort" }}}
              {"$sort" : {"$hybrid" : {"$vector" : [1,2,3] , "$lexical": "lexical sort" }}}
      """)
public record FindAndRerankSort(String vectorizeSort, String lexicalSort, float[] vectorSort)
    implements Recordable {

  static final FindAndRerankSort NO_ARG_SORT = new FindAndRerankSort(null, null, null);

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("vectorizeSort", vectorizeSort)
        .append("lexicalSort", lexicalSort)
        .append("vectorSort", Arrays.toString(vectorSort));
  }

  /**
   * Override to do a value equality check on the vector
   *
   * @param obj the reference object with which to compare.
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    return Objects.equals(vectorizeSort, ((FindAndRerankSort) obj).vectorizeSort)
        && Objects.equals(lexicalSort, ((FindAndRerankSort) obj).lexicalSort)
        && Arrays.equals(vectorSort, ((FindAndRerankSort) obj).vectorSort);
  }

  /** Override to do a value equality hash on the vector */
  @Override
  public int hashCode() {
    return Objects.hash(vectorizeSort, lexicalSort, Arrays.hashCode(vectorSort));
  }
}
