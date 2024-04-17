package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Helper class that implements functionality needed to support projections on documents fetched via
 * various {@code find} commands.
 *
 * <p>Note that this is not used for "No Index" handling: see {@link IndexingProjector} for that use
 * case
 */
public class DocumentProjector {
  /**
   * No-op projector that does not modify documents. Considered "exclusion" projector since "no
   * exclusions" is conceptually what happens ("no inclusions" would drop all content)
   */
  private static final DocumentProjector INCLUDE_ALL_PROJECTOR =
      new DocumentProjector(null, false, false);

  private static final DocumentProjector INCLUDE_ALL_PROJECTOR_WITH_SIMILARITY =
      new DocumentProjector(null, false, true);

  private static final DocumentProjector EXCLUDE_ALL_PROJECTOR =
      new DocumentProjector(null, true, false);

  private static final DocumentProjector EXCLUDE_ALL_PROJECTOR_WITH_SIMILARITY =
      new DocumentProjector(null, true, true);

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

  public static DocumentProjector defaultProjector() {
    return DefaultProjectorWrapper.defaultProjector();
  }

  public static DocumentProjector defaultProjectorWithSimilarity() {
    return DefaultProjectorWrapper.defaultProjectorWithSimilarity();
  }

  public static DocumentProjector includeAllProjector() {
    return INCLUDE_ALL_PROJECTOR;
  }

  DocumentProjector withIncludeSimilarity(boolean includeSimilarityScore) {
    if (this.includeSimilarityScore == includeSimilarityScore) {
      return this;
    }
    return new DocumentProjector(rootLayer, inclusion, includeSimilarityScore);
  }

  public static DocumentProjector createFromDefinition(JsonNode projectionDefinition) {
    return createFromDefinition(projectionDefinition, false);
  }

  public static DocumentProjector createFromDefinition(
      JsonNode projectionDefinition, boolean includeSimilarity) {
    // First special case: "simple" default projection
    if (projectionDefinition == null || projectionDefinition.isEmpty()) {
      if (includeSimilarity) {
        return defaultProjectorWithSimilarity();
      }
      return defaultProjector();
    }
    if (!projectionDefinition.isObject()) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": definition must be OBJECT, was "
              + projectionDefinition.getNodeType());
    }
    // Special cases: "star-include/exclude"
    if (projectionDefinition.size() == 1) {
      Map.Entry<String, JsonNode> entry = projectionDefinition.fields().next();
      if ("*".equals(entry.getKey())) {
        boolean includeAll = extractIncludeOrExclude(entry.getKey(), entry.getValue());
        if (includeAll) {
          return includeSimilarity ? INCLUDE_ALL_PROJECTOR_WITH_SIMILARITY : INCLUDE_ALL_PROJECTOR;
        }
        return includeSimilarity ? EXCLUDE_ALL_PROJECTOR_WITH_SIMILARITY : EXCLUDE_ALL_PROJECTOR;
      }
    }
    return PathCollector.collectPaths(projectionDefinition, includeSimilarity).buildProjector();
  }

  private static boolean extractIncludeOrExclude(String path, JsonNode value) {
    if (value.isNumber()) {
      // "0" means exclude (like false); any other number include
      return !BigDecimal.ZERO.equals(value.decimalValue());
    }
    if (value.isBoolean()) {
      return value.booleanValue();
    }
    // Unknown JSON node type; error
    throw new JsonApiException(
        ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
        ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
            + ": path ('"
            + path
            + "') value must be NUMBER or BOOLEAN, was "
            + value.getNodeType());
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
    Objects.requireNonNull(document, "Document to call 'applyProjection()' on must not be null");
    // null -> either include-add or exclude-all; but logic may seem counter-intuitive
    if (rootLayer == null) {
      if (inclusion) { // exclude-all
        ((ObjectNode) document).removeAll();
      }
      // In either case, we may need to add similarity score if present
      if (includeSimilarityScore && similarityScore != null) {
        ((ObjectNode) document)
            .put(DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD, similarityScore);
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
          .put(DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD, similarityScore);
    }
  }

  // Mostly for deserialization tests
  @Override
  public boolean equals(Object o) {
    if (o instanceof DocumentProjector) {
      DocumentProjector other = (DocumentProjector) o;
      return (this.inclusion == other.inclusion)
          && (this.includeSimilarityScore == other.includeSimilarityScore)
          && Objects.equals(this.rootLayer, other.rootLayer);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (rootLayer == null) ? -1 : rootLayer.hashCode();
  }

  /**
   * Due to the way projection is handled, we need to handle construction of default instance via
   * separate class (to avoid cyclic dependency)
   */
  static class DefaultProjectorWrapper {
    /**
     * Default projector that drops $vector and $vectorize fields but otherwise leaves document
     * as-is. Constructed from empty definition (no inclusions/exclusions).
     */
    private static final DocumentProjector DEFAULT_PROJECTOR;

    static {
      ObjectNode emptyDef = new ObjectNode(JsonNodeFactory.instance);
      DEFAULT_PROJECTOR = PathCollector.collectPaths(emptyDef, false).buildProjector();
    }

    private static final DocumentProjector DEFAULT_PROJECTOR_WITH_SIMILARITY =
        DEFAULT_PROJECTOR.withIncludeSimilarity(true);

    public static DocumentProjector defaultProjector() {
      return DEFAULT_PROJECTOR;
    }

    public static DocumentProjector defaultProjectorWithSimilarity() {
      return DEFAULT_PROJECTOR_WITH_SIMILARITY;
    }
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

    private Boolean idInclusion;

    private Boolean $vectorInclusion;

    private Boolean $vectorizeInclusion;

    /** Whether similarity score is needed. */
    private final boolean includeSimilarityScore;

    private PathCollector(boolean includeSimilarityScore) {
      this.includeSimilarityScore = includeSimilarityScore;
    }

    static PathCollector collectPaths(JsonNode def, boolean includeSimilarity) {
      return new PathCollector(includeSimilarity).collectFromObject(def, null);
    }

    public DocumentProjector buildProjector() {
      // One more thing: do we need to add document id?
      if (inclusions > 0) { // inclusion-based projection
        return new DocumentProjector(
            ProjectionLayer.buildLayersForProjection(
                paths,
                slices,
                // doc-id included unless explicitly excluded
                !Boolean.FALSE.equals(idInclusion),
                // $vector only included if explicitly included
                Boolean.TRUE.equals($vectorInclusion),
                // $vectorize only included if explicitly included
                Boolean.TRUE.equals($vectorizeInclusion)),
            true,
            includeSimilarityScore);
      } else { // exclusion-based
        return new DocumentProjector(
            ProjectionLayer.buildLayersForProjection(
                paths,
                slices,
                // doc-id excluded only if explicitly excluded
                Boolean.FALSE.equals(idInclusion),
                // $vector excluded unless explicitly included
                !Boolean.TRUE.equals($vectorInclusion),
                // $vectorize excluded unless explicitly included
                !Boolean.TRUE.equals($vectorizeInclusion)),
            false,
            includeSimilarityScore);
      }
    }

    PathCollector collectFromObject(JsonNode ob, String parentPath) {
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
                || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path))) {
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

        // Special rule for "*": only allowed as single root-level entry;
        if ("*".equals(path)) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": wildcard ('*') only allowed as the only root-level path");
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
          collectFromObject(value, path);
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
      } else if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)) {
        $vectorInclusion = false;
      } else if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path)) {
        $vectorizeInclusion = false;
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
      } else if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)) {
        $vectorInclusion = true;
      } else if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path)) {
        $vectorizeInclusion = true;
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
