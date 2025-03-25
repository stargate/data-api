package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;

/** Description of the {@link DocumentScores} to be used when returning them on the API. */
public class DocumentScoresDesc {

  private final Scores scores;

  public DocumentScoresDesc(float vector, float rerank) {
    this.scores = new Scores(vector, rerank);
  }

  @JsonProperty(DocumentConstants.Fields.SCORES_FIELD)
  public Scores scores() {
    return scores;
  }

  @JsonPropertyOrder({
    DocumentConstants.Fields.RERANK_FIELD,
    DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD
  })
  public record Scores(
      @JsonProperty(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD) float vector,
      @JsonProperty(DocumentConstants.Fields.RERANK_FIELD) float rerank) {}
}
