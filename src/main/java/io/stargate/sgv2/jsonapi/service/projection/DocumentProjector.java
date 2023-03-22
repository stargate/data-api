package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

/**
 * Helper class that implements functionality needed to support projections on documents fetched via
 * various {@code find} commands.
 */
public class DocumentProjector {
  private DocumentProjector() {}

  public static DocumentProjector createFromDefinition(JsonNode projectionDefinition) {
    if (projectionDefinition == null) {
      return null;
    }
    if (!projectionDefinition.isObject()) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": definition must be OBJECT, was "
              + projectionDefinition.getNodeType());
    }
    if (projectionDefinition.isEmpty()) {
      return null;
    }
    return new DocumentProjector();
  }

  public void applyProjection(JsonNode document) {
    ; // To implement
  }
}
