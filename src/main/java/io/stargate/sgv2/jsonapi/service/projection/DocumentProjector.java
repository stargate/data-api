package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Helper class that implements functionality needed to support projections on documents fetched via
 * various {@code find} commands.
 */
public class DocumentProjector {
  /**
   * No-op projector that does not modify documents. Considered "exclusion" projector since "no
   * exclusions" is conceptually what happens ("no inclusions" would drop all content)
   */
  private static final DocumentProjector IDENTITY_PROJECTOR =
      new DocumentProjector(null, false, false);

  private static final DocumentProjector IDENTITY_PROJECTOR_WITH_SIMILARITY =
      new DocumentProjector(null, false, true);

  private final ProjectionLayer rootLayer;

  /** Whether this projector is inclusion- ({@code true}) or exclusion ({@code false}) based. */
  private final boolean inclusion;

  /** Whether to include the similarity score in the projection. */
  private final boolean includeSimilarityScore;

  private DocumentProjector(
      ProjectionLayer rootLayer, boolean inclusion, boolean includeSimilarityScore) {
    this.rootLayer = rootLayer;
    this.inclusion = inclusion;
    this.includeSimilarityScore = includeSimilarityScore;
  }

  public static DocumentProjector createFromDefinition(JsonNode projectionDefinition) {
    return createFromDefinition(projectionDefinition, false);
  }

  public static DocumentProjector createFromDefinition(
      JsonNode projectionDefinition, boolean includeSimilarity) {
    if (projectionDefinition == null) {
      if (includeSimilarity) {
        return IDENTITY_PROJECTOR_WITH_SIMILARITY;
      } else {
        return identityProjector();
      }
    }
    if (!projectionDefinition.isObject()) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": definition must be OBJECT, was "
              + projectionDefinition.getNodeType());
    }
    return PathCollector.collectPaths(projectionDefinition, includeSimilarity).buildProjector();
  }

  public static DocumentProjector identityProjector() {
    return IDENTITY_PROJECTOR;
  }

  public static DocumentProjector getIdentityProjectorWithSimilarity() {
    return IDENTITY_PROJECTOR;
  }

  public boolean isInclusion() {
    return inclusion;
  }

  public boolean doIncludeSimilarityScore() {
    return includeSimilarityScore;
  }

  public void applyProjection(JsonNode document) {
    applyProjection(document, null);
  }

  public void applyProjection(JsonNode document, Float similarityScore) {
    if (rootLayer == null) { // null -> identity projection (no-op)
      if (includeSimilarityScore && similarityScore != null) {
        ((ObjectNode) document)
            .put(DocumentConstants.Fields.VECTOR_FUNCTION_PROJECTION_FIELD, similarityScore);
      }
      return;
    }
    if (inclusion) {
      rootLayer.applyInclusions(document);
    } else {
      rootLayer.applyExclusions(document);
    }
    if (includeSimilarityScore && similarityScore != null) {
      ((ObjectNode) document)
          .put(DocumentConstants.Fields.VECTOR_FUNCTION_PROJECTION_FIELD, similarityScore);
    }
  }

  // Mostly for deserialization tests
  @Override
  public boolean equals(Object o) {
    if (o instanceof DocumentProjector) {
      DocumentProjector other = (DocumentProjector) o;
      return (this.inclusion == other.inclusion) && Objects.equals(this.rootLayer, other.rootLayer);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return rootLayer.hashCode();
  }

  /**
   * Helper object used to traverse and collection inclusion/exclusion path definitions and verify
   * that there are only one or the other (except for doc id). Does not build data structures for
   * actual matching.
   */
  private static class PathCollector {
    private List<String> paths = new ArrayList<>();

    private List<ProjectionLayer.SliceDef> slices = new ArrayList<>();

    private int exclusions, inclusions;

    private Boolean idInclusion = null;

    /** Whether similarity score is needed. */
    private boolean includeSimilarityScore;

    private PathCollector() {}

    static PathCollector collectPaths(JsonNode def, boolean includeSimilarity) {
      return new PathCollector().collectFromObject(def, null, includeSimilarity);
    }

    public DocumentProjector buildProjector() {
      if (isIdentityProjection()) {
        return identityProjector();
      }

      // One more thing: do we need to add document id?
      if (inclusions > 0) { // inclusion-based projection
        // doc-id included unless explicitly excluded
        return new DocumentProjector(
            ProjectionLayer.buildLayers(paths, slices, !Boolean.FALSE.equals(idInclusion)),
            true,
            includeSimilarityScore);
      } else { // exclusion-based
        // doc-id excluded only if explicitly excluded
        return new DocumentProjector(
            ProjectionLayer.buildLayers(paths, slices, Boolean.FALSE.equals(idInclusion)),
            false,
            includeSimilarityScore);
      }
    }

    /**
     * Accessor to use for checking if collected paths indicate "empty" (no-operation) projection:
     * if so, caller can avoid actual construction or evaluation.
     */
    boolean isIdentityProjection() {
      // Only the case if we have no non-doc-id inclusions/exclusions AND
      // doc-id is included (by default or explicitly)
      return paths.isEmpty() && slices.isEmpty() && !Boolean.FALSE.equals(idInclusion);
    }

    PathCollector collectFromObject(JsonNode ob, String parentPath, boolean includeSimilarity) {
      var it = ob.fields();
      while (it.hasNext()) {
        var entry = it.next();
        String path = entry.getKey();

        if (path.isEmpty()) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": empty paths (and path segments) not allowed");
        }
        if (path.charAt(0) == '$'
            && !(path.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)
                || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path)
                || DocumentConstants.Fields.VECTOR_FUNCTION_PROJECTION_FIELD.equals(path))) {
          // First: no operators allowed at root level
          if (parentPath == null) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": path cannot start with '$' (no root-level operators)");
          }

          // Second: we only support one operator for now
          if (!"$slice".equals(path)) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": unrecognized/unsupported projection operator '"
                    + path
                    + "' (only '$slice' supported)");
          }

          addSlice(parentPath, entry.getValue());
          continue;
        }
        // This `or` is needed because the method is called in loop
        includeSimilarityScore = includeSimilarityScore || includeSimilarity;
        if (parentPath == null
            && DocumentConstants.Fields.VECTOR_FUNCTION_PROJECTION_FIELD.equals(path)) {
          JsonNode value = entry.getValue();
          if (BigDecimal.ZERO.equals(value.decimalValue()) && includeSimilarity) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": Cannot exclude $similarity when `includeSimilarity` option is set `true`");
          } else {
            includeSimilarityScore = true;
          }
        }

        if (parentPath != null) {
          path = parentPath + "." + path;
        }
        JsonNode value = entry.getValue();
        if (value.isNumber()) {
          // "0" means exclude (like false); any other number include
          if (BigDecimal.ZERO.equals(value.decimalValue())) {
            addExclusion(path);
          } else {
            addInclusion(path);
          }
        } else if (value.isBoolean()) {
          if (value.booleanValue()) {
            addInclusion(path);
          } else {
            addExclusion(path);
          }
        } else if (value.isObject()) {
          // Nested definitions allowed, too
          collectFromObject(value, path, includeSimilarity);
        } else {
          // Unknown JSON node type; error
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": path ('"
                  + path
                  + "') value must be NUMBER, BOOLEAN or OBJECT, was "
                  + value.getNodeType());
        }
      }
      return this;
    }

    private void addSlice(String path, JsonNode sliceDef) {
      if (sliceDef.isArray()) {
        if (sliceDef.size() == 2
            && sliceDef.get(0).isIntegralNumber()
            && sliceDef.get(1).isIntegralNumber()) {
          int skip = sliceDef.get(0).intValue();
          int count = sliceDef.get(1).intValue();
          if (count < 0) { // negative values not allowed
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": path ('"
                    + path
                    + "') has unsupported parameter for '$slice' ("
                    + sliceDef.getNodeType()
                    + "): second NUMBER (entries to return) MUST be positive");
          }
          slices.add(
              new ProjectionLayer.SliceDef(path, ProjectionLayer.constructSlicer(skip, count)));
          return;
        }
      } else if (sliceDef.isIntegralNumber()) {
        int count = sliceDef.intValue();
        slices.add(new ProjectionLayer.SliceDef(path, ProjectionLayer.constructSlicer(count)));
        return;
      }
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": path ('"
              + path
              + "') has unsupported parameter for '$slice' ("
              + sliceDef.getNodeType()
              + "): only NUMBER or ARRAY with 2 NUMBERs accepted");
    }

    private void addExclusion(String path) {
      if (DocumentConstants.Fields.DOC_ID.equals(path)) {
        idInclusion = false;
      } else {
        // Must not mix exclusions and inclusions
        if (inclusions > 0) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": cannot exclude '"
                  + path
                  + "' on inclusion projection");
        }
        ++exclusions;
        paths.add(path);
      }
    }

    private void addInclusion(String path) {
      if (DocumentConstants.Fields.DOC_ID.equals(path)) {
        idInclusion = true;
      } else {
        // Must not mix exclusions and inclusions
        if (exclusions > 0) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": cannot include '"
                  + path
                  + "' on exclusion projection");
        }
        ++inclusions;
        paths.add(path);
      }
    }
  }
}
