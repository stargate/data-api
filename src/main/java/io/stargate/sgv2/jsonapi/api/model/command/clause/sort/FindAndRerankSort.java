package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import static io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider.type;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;
import static org.eclipse.microprofile.openapi.models.media.Schema.SchemaType.*;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.eclipse.microprofile.openapi.OASFactory;
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
    description =
        "Use $hybrid with a query string or an object containing $vectorize or $vector, and optionally $lexical."
            + " $vector and $vectorize cannot be used together."
            + " Query text and required collection capabilities are checked when resolving the command.",
    implementation = FindAndRerankSort.SchemaProvider.class,
    ref = FindAndRerankSort.SchemaProvider.REF,
    examples = {
      """
      {"$hybrid": "Same query for vectorize and BM25 sorting"}
      """,
      """
      {"$hybrid": {"$vectorize": "vectorize sort query", "$lexical": "lexical sort"}}
      """,
      """
      {"$hybrid": {"$vector": [1, 2, 3], "$lexical": "lexical sort"}}
      """
    })
public record FindAndRerankSort(
    String vectorizeSort, String lexicalSort, float[] vectorSort, CommandFeatures commandFeatures)
    implements Recordable {

  @Schema(name = SchemaProvider.NAME)
  @RegisterForReflection
  public static class SchemaProvider implements ApiSchemaProvider {
    public static final String NAME = "FindAndRerankSort";
    public static final String REF = "#/components/schemas/" + NAME;

    @Override
    public org.eclipse.microprofile.openapi.models.media.Schema schema() {
      var vector =
          OASFactory.createSchema()
              .addAnyOf(type(ARRAY).items(type(NUMBER)))
              .addAnyOf(
                  type(OBJECT)
                      .addProperty(BINARY_VECTOR_TEXT_FIELD, type(STRING))
                      .addRequired(BINARY_VECTOR_TEXT_FIELD)
                      .additionalPropertiesBoolean(false))
              .addAnyOf(type(NULL));
      var hybrid =
          OASFactory.createSchema()
              .addAnyOf(type(STRING))
              .addAnyOf(
                  type(OBJECT)
                      .addProperty(VECTOR_EMBEDDING_TEXT_FIELD, type(STRING, NULL))
                      .addProperty(LEXICAL_CONTENT_FIELD, type(STRING, NULL))
                      .addProperty(VECTOR_EMBEDDING_FIELD, vector)
                      .additionalPropertiesBoolean(false)
                      .not(
                          OASFactory.createSchema()
                              .required(
                                  List.of(VECTOR_EMBEDDING_FIELD, VECTOR_EMBEDDING_TEXT_FIELD))));
      return type(OBJECT, NULL)
          .description(FindAndRerankSort.class.getAnnotation(Schema.class).description())
          .addProperty(HYBRID_FIELD, hybrid)
          .additionalPropertiesBoolean(false)
          .addExample(Map.of(HYBRID_FIELD, "search text"))
          .addExample(
              Map.of(
                  HYBRID_FIELD,
                  Map.of(
                      VECTOR_EMBEDDING_TEXT_FIELD,
                      "vector query",
                      LEXICAL_CONTENT_FIELD,
                      "lexical query")))
          .addExample(
              Map.of(
                  HYBRID_FIELD,
                  Map.of(
                      VECTOR_EMBEDDING_FIELD,
                      List.of(0.1, 0.2, 0.3),
                      LEXICAL_CONTENT_FIELD,
                      "lexical query")))
          .addExample(
              Map.of(
                  HYBRID_FIELD,
                  Map.of(
                      VECTOR_EMBEDDING_FIELD,
                      Map.of(BINARY_VECTOR_TEXT_FIELD, "P4AAAEAAAABAQAAA"),
                      LEXICAL_CONTENT_FIELD,
                      "lexical query")));
    }
  }

  public static final FindAndRerankSort NO_ARG_SORT =
      new FindAndRerankSort(null, null, null, CommandFeatures.EMPTY);

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
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
        && Objects.equals(vectorizeSort, other.vectorizeSort)
        && Objects.equals(lexicalSort, other.lexicalSort)
        && Arrays.equals(vectorSort, other.vectorSort)
        && Objects.equals(commandFeatures, other.commandFeatures);
  }

  /** Override to do a value equality hash on the vector */
  @Override
  public int hashCode() {
    return Objects.hash(vectorizeSort, lexicalSort, Arrays.hashCode(vectorSort), commandFeatures);
  }
}
