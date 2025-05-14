package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

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
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterSpec;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.metrics.CommandFeature;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.metrics.FeatureSource;
import io.stargate.sgv2.jsonapi.util.JsonFieldMatcher;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Finds documents using using vector and lexical sorting, then reranks the results.")
@JsonTypeName(CommandName.Names.FIND_AND_RERANK)
public record FindAndRerankCommand(
    @Valid @JsonProperty("filter") FilterSpec filterSpec,
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
  public void addCommandFeaturesToCommandContext(CommandContext<?> context) {
    context
        .commandFeatures()
        .addAll((sortClause != null) ? sortClause.getCommandFeatures() : CommandFeatures.EMPTY);
    context
        .commandFeatures()
        .addAll(
            (options != null && options.hybridLimits() != null)
                ? options.hybridLimits().getCommandFeatures()
                : CommandFeatures.EMPTY);
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
                  "The maximum number of documents to read for the vector and lexical queries that feed into the reranking. May be an number or an object with $vector and $lexical fields.",
              example =
                  """
                {"hybridLimits" : 100}
                {"hybridLimits" : {"$vector" : 100, "$lexical" : 10}}
                """)
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
          boolean includeSortVector) {}

  @JsonDeserialize(using = HybridLimitsDeserializer.class)
  public record HybridLimits(
      @JsonProperty(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD) int vectorLimit,
      /** ---- */
      @JsonProperty(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD) int lexicalLimit,
      CommandFeatures commandFeatures)
      implements Recordable, FeatureSource {
    public static final HybridLimits DEFAULT = new HybridLimits(50, 50, CommandFeatures.EMPTY);

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("vectorLimit", vectorLimit)
          .append("lexicalLimit", lexicalLimit)
          .append("commandFeatures", commandFeatures);
    }

    @Override
    public CommandFeatures getCommandFeatures() {
      return commandFeatures != null ? commandFeatures : CommandFeatures.EMPTY;
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

    private HybridLimits deserialize(JsonParser jsonParser, NumericNode limitsNumber)
        throws JsonMappingException {

      return new HybridLimits(
          normaliseLimit(jsonParser, limitsNumber, VECTOR_EMBEDDING_FIELD),
          normaliseLimit(jsonParser, limitsNumber, LEXICAL_CONTENT_FIELD),
          CommandFeatures.of(CommandFeature.HYBRID_LIMITS_NUMBER));
    }

    private HybridLimits deserialize(JsonParser jsonParser, ObjectNode limitsObject)
        throws JsonMappingException {

      var limitMatch = MATCH_LIMIT_FIELDS.matchAndThrow(limitsObject, jsonParser, ERROR_CONTEXT);

      return new HybridLimits(
          normaliseLimit(
              jsonParser, limitMatch.matched().get(VECTOR_EMBEDDING_FIELD), VECTOR_EMBEDDING_FIELD),
          normaliseLimit(
              jsonParser, limitMatch.matched().get(LEXICAL_CONTENT_FIELD), LEXICAL_CONTENT_FIELD),
          CommandFeatures.of(
              CommandFeature.HYBRID_LIMITS_VECTOR, CommandFeature.HYBRID_LIMITS_LEXICAL));
    }

    private int normaliseLimit(JsonParser jsonParser, NumericNode limitNode, String fieldName)
        throws JsonMappingException {
      int limit = limitNode.asInt();

      if (limit < 0) {
        throw new JsonMappingException(
            jsonParser,
            "hybridLimits must be zero or greater, got %s for %s".formatted(limit, fieldName));
      }
      return limit;
    }
  }
}
