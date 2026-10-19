package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider.type;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;
import static org.eclipse.microprofile.openapi.models.media.Schema.SchemaType.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.util.JsonFieldMatcher;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.OASFactory;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Finds documents using vector and lexical sorting, then reranks the results.")
@JsonTypeName(CommandName.Names.FIND_AND_RERANK)
public record FindAndRerankCommand(
    @Valid @JsonProperty("filter") FilterDefinition filterDefinition,
    @JsonProperty("projection") JsonNode projectionDefinition,
    @Valid @JsonProperty("sort") FindAndRerankSort sortClause,
    @Valid @Nullable Options options)
    implements ReadCommand, Filterable, Projectable, Windowable {

  public FindAndRerankCommand {
    sortClause = (sortClause == null) ? FindAndRerankSort.NO_ARG_SORT : sortClause;
  }

  // NOTE: is not VectorSortable because it has its own sort clause.

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND_AND_RERANK;
  }

  @Override
  public void addCommandFeatures(CommandFeatures commandFeatures) {
    if (sortClause != null) {
      commandFeatures.addAll(sortClause.commandFeatures());
    }
    if (options != null && options.hybridLimits() != null) {
      commandFeatures.addAll(options.hybridLimits().commandFeatures());
    }
  }

  public record Options(
      @Positive(message = "limit should be greater than `0`")
          @Schema(
              description = "The maximum number of documents to return after reranking.",
              type = SchemaType.INTEGER,
              implementation = Integer.class)
          Integer limit,
      /** ---- */
      @Schema(
              description =
                  "The maximum number of documents to read for the vector and lexical queries that feed into the reranking. May be a number or an object with $vector and $lexical fields. The accepted range is determined by server configuration.",
              ref = HybridLimits.SchemaProvider.REF,
              examples = {
                "50",
                """
                {"$vector": 50, "$lexical": 10}
                """
              })
          HybridLimits hybridLimits,
      /** ---- */
      @Schema(
              description =
                  "Query to pass to the reranking model, defaults to the text used for the $vectorize sort, required if $vectorize is not used.",
              type = SchemaType.STRING)
          String rerankQuery,
      /** ---- */
      @Schema(
              description =
                  "Name of the Document Field that contains the passage to rerank on, defaults to "
                      + DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD
                      + " when that is used for sorting, required when other sort is used.",
              type = SchemaType.STRING,
              defaultValue = DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)
          String rerankOn,
      /** ---- */
      @Schema(
              description =
                  "Include the scores from vectors and reranking in the response, defaults to false.",
              type = SchemaType.BOOLEAN,
              defaultValue = "false")
          boolean includeScores,
      /** ---- */
      @Schema(
              description = "Return vector embedding used for ANN sorting.",
              type = SchemaType.BOOLEAN)
          boolean includeSortVector,
      // Note: intentionally NOT annotated @Valid. Jakarta Bean Validation would reject any
      // missing/null provider with its generic violation message; we want the explicit
      // INVALID_RERANK_OVERRIDE error code produced by CollectionRerankDef.validateServiceDesc.
      // Any non-null payload — including `"rerank": {}` and payloads whose fields are all
      // explicit nulls — is treated as an override attempt and validated there.
      @Nullable
          @Schema(
              description =
                  "Optional override for the reranking service. Completely replaces the"
                      + " collection-level reranking configuration when provided. Both 'provider' and"
                      + " 'modelName' are required.",
              implementation = RerankOverrideSchema.class,
              ref = RerankOverrideSchema.REF)
          @JsonProperty("rerank")
          CreateCollectionCommand.Options.RerankServiceDesc rerankServiceOverride) {}

  // Collection creation uses the same service record but also accepts authentication and
  // parameters.
  @Schema(name = RerankOverrideSchema.NAME)
  @RegisterForReflection
  public static class RerankOverrideSchema implements ApiSchemaProvider {
    public static final String NAME = "RerankServiceOverride";
    public static final String REF = "#/components/schemas/" + NAME;

    @Override
    public org.eclipse.microprofile.openapi.models.media.Schema schema() {
      return type(OBJECT, NULL)
          .addProperty("provider", type(STRING))
          .addProperty("modelName", type(STRING))
          .required(List.of("provider", "modelName"))
          .additionalPropertiesBoolean(false)
          .addExample(
              Map.of("provider", "nvidia", "modelName", "nvidia/llama-3.2-nv-rerankqa-1b-v2"));
    }
  }

  @JsonDeserialize(using = HybridLimitsDeserializer.class)
  @Schema(
      description =
          "Candidate read limits as one integer for both legs, or an object containing both $vector and $lexical integer limits. Accepted bounds are determined by server configuration.",
      implementation = HybridLimits.SchemaProvider.class,
      ref = HybridLimits.SchemaProvider.REF)
  public record HybridLimits(
      @JsonProperty(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD) int vectorLimit,
      /** ---- */
      @JsonProperty(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD) int lexicalLimit,
      CommandFeatures commandFeatures)
      implements Recordable {

    @Schema(name = SchemaProvider.NAME)
    @RegisterForReflection
    public static class SchemaProvider implements ApiSchemaProvider {
      public static final String NAME = "HybridLimits";
      public static final String REF = "#/components/schemas/" + NAME;

      @Override
      public org.eclipse.microprofile.openapi.models.media.Schema schema() {
        return OASFactory.createSchema()
            .description(HybridLimits.class.getAnnotation(Schema.class).description())
            .addAnyOf(type(INTEGER))
            .addAnyOf(
                type(OBJECT)
                    .addProperty(VECTOR_EMBEDDING_FIELD, type(INTEGER))
                    .addProperty(LEXICAL_CONTENT_FIELD, type(INTEGER))
                    .required(List.of(VECTOR_EMBEDDING_FIELD, LEXICAL_CONTENT_FIELD))
                    .additionalPropertiesBoolean(false))
            .addAnyOf(type(NULL))
            .addExample(50)
            .addExample(Map.of(VECTOR_EMBEDDING_FIELD, 50, LEXICAL_CONTENT_FIELD, 10));
      }
    }

    public static final HybridLimits DEFAULT =
        new HybridLimits(
            RerankingConstants.HybridSearchLimits.DEFAULT,
            RerankingConstants.HybridSearchLimits.DEFAULT,
            CommandFeatures.EMPTY);

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("vectorLimit", vectorLimit)
          .append("lexicalLimit", lexicalLimit)
          .append("commandFeatures", commandFeatures);
    }
  }

  /** Deserializer for the `hybridLimits` option, the limits for the inner reads. */
  public static class HybridLimitsDeserializer extends StdDeserializer<HybridLimits> {

    private static final String ERROR_CONTEXT = "hybridLimits";

    // user can specify the limit as a single number node, or as an object with $vector and $lexical
    // this is macher for the later
    //
    // {"options": {"hybridLimits" : 100}}
    // or, both fields are required
    // {"options": {"hybridLimits" : {"$vector" : 100, "$lexical" : 10}}
    private static final JsonFieldMatcher<NumericNode> MATCH_LIMIT_FIELDS =
        new JsonFieldMatcher<>(
            NumericNode.class, List.of(LEXICAL_CONTENT_FIELD, VECTOR_EMBEDDING_FIELD), List.of());

    protected HybridLimitsDeserializer() {
      super(HybridLimits.class);
    }

    @Override
    public HybridLimits deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

      return switch (deserializationContext.readTree(jsonParser)) {
        case NumericNode number -> deserialize(jsonParser, number);
        case ObjectNode object -> deserialize(jsonParser, object);
        case JsonNode node ->
            throw new JsonMappingException(
                jsonParser,
                "hybridLimits must be an integer or an object, got %s"
                    .formatted(node.getNodeType()));
      };
    }

    // Range validation against dynamic OperationsConfig bounds happens later in
    // FindAndRerankOperationBuilder.build(); the deserializer only handles JSON shape.
    private HybridLimits deserialize(JsonParser jsonParser, NumericNode limitsNumber) {
      var limit = limitsNumber.asInt();
      return new HybridLimits(
          limit, limit, CommandFeatures.of(CommandFeature.HYBRID_LIMITS_NUMBER));
    }

    private HybridLimits deserialize(JsonParser jsonParser, ObjectNode limitsObject)
        throws JsonMappingException {

      var limitMatch = MATCH_LIMIT_FIELDS.matchAndThrow(limitsObject, jsonParser, ERROR_CONTEXT);

      return new HybridLimits(
          limitMatch.matched().get(VECTOR_EMBEDDING_FIELD).asInt(),
          limitMatch.matched().get(LEXICAL_CONTENT_FIELD).asInt(),
          CommandFeatures.of(
              CommandFeature.HYBRID_LIMITS_VECTOR, CommandFeature.HYBRID_LIMITS_LEXICAL));
    }
  }
}
