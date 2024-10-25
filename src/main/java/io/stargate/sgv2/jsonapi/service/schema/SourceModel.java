package io.stargate.sgv2.jsonapi.service.schema;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The source model used for the vector index. This is only applicable if the vector index is
 * enabled.
 */
public enum SourceModel {
  ADA002("ada002"),
  BERT("bert"),
  COHERE_V3("cohere_v3"),
  GECKO("gecko"),
  NV_QA_4("nv_qa_4"),
  OPENAI_V3_LARGE("openai_v3_large"),
  OPENAI_V3_SMALL("openai_v3_small"),
  OTHER("other"),
  UNDEFINED("undefined");

  private final String sourceModel;
  private static final java.util.Map<String, SourceModel> FUNCTIONS_MAP =
      Stream.of(SourceModel.values())
          .collect(Collectors.toMap(SourceModel::getSourceModel, sourceModel -> sourceModel));

  SourceModel(String sourceModel) {
    this.sourceModel = sourceModel;
  }

  public String getSourceModel() {
    return sourceModel;
  }

  public static SourceModel fromString(String sourceModel) {
    if (sourceModel == null) return UNDEFINED;
    // The string may be empty if the collection was created before supporting source models
    if (sourceModel.isEmpty()) return OTHER;
    SourceModel model = FUNCTIONS_MAP.get(sourceModel);
    if (model == null) {
      throw ErrorCodeV1.VECTOR_SEARCH_INVALID_SOURCE_MODEL_NAME.toApiException("'%s'", sourceModel);
    }
    return model;
  }
}
