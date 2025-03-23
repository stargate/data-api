package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Map;
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
              {"$hybrid" : "Same query for vectorize and bm25 sorting"}
              {"$hybrid" : {"$vectorize" : "vectorize sort query" , "$lexical": "lexical sort" }}
              {"$hybrid" : {"$vector" : [1,2,3] , "$lexical": "lexical sort" }}
      """)
public record FindAndRerankSort(String vectorizeSort, String lexicalSort, float[] vectorSort) {

  static final FindAndRerankSort NO_ARG_SORT = new FindAndRerankSort(null, null, null);
}
