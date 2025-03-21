package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import java.io.IOException;
import java.util.Optional;
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

  // NOTE: is not VectorSortable because it has its own sort clause.

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.FIND;
  }

  @Override
  public Optional<Integer> limit() {
    return options() == null ? Optional.empty() : Optional.of(options().limit());
  }

  public Optional<Boolean> includeSortVector() {
    return options() == null ? Optional.empty() : Optional.of(options().includeSortVector);
  }

  public record Options(
      @Positive(message = "limit should be greater than `0`")
          @Schema(
              description =
                  "The maximum number of documents to return after the reranking service has ranked them.",
              type = SchemaType.INTEGER,
              implementation = Integer.class)
          Integer limit,
      @Positive(message = "limit should be greater than `0`")
          @Schema(
              description =
                  "The maximum number of documents to read for the vector and lexical queries that feed into the reranking.",
              type = SchemaType.INTEGER,
              implementation = Integer.class)
          int hybridLimits,
      @Schema(
              description =
                  "Name of the Document Field that contains the passage we want to rerank on, defaults to "
                      + DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD
                      + ".",
              type = SchemaType.STRING,
              defaultValue = DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)
          boolean rerankOn,
      @Schema(
              description = "Include the scores from vectors and reranking in the response.",
              type = SchemaType.BOOLEAN)
          boolean includeScores,
      @Schema(
              description = "Return vector embedding used for ANN sorting.",
              type = SchemaType.BOOLEAN)
          boolean includeSortVector) {}

  @JsonDeserialize(using = HybridLimitsDeserializer.class)
  public record HybridLimits(
      @JsonProperty(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD) int vectorLimit,
      // TODO: AARON : Get the constant for $lexical
      @JsonProperty("$lexical") int lexicalLimit) {}

  /**
   * Deserializer for the `hybridLimits` option, the limits for the inner reads.
   *
   * <p>The limit can be:
   *
   * <pre>
   *   {
   *     "options" : {
   *       // same limit for the vector and lexical reads
   *       "hybridLimits" : 100
   *       // different limits for the vector and lexical reads
   *       "hybridLimits" : {
   *          "$vector" : 100,
   *          "$lexical" : 10
   *     }
   *   }
   * </pre>
   */
  static class HybridLimitsDeserializer extends StdDeserializer<HybridLimits> {

    protected HybridLimitsDeserializer() {
      super(HybridLimits.class);
    }

    @Override
    public HybridLimits deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {

      return switch (deserializationContext.readTree(jsonParser)) {
        case NumericNode number -> new HybridLimits(number.asInt(), number.asInt());
        case ObjectNode object ->
            deserializationContext.readTreeAsValue(object, HybridLimits.class);
        default ->
            throw new JsonMappingException(
                jsonParser,
                "hybridLimits must be an integer or an object with $vector and $lexical fields");
      };
    }
  }
}
