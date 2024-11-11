package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Definition of vector config for a collection or table */
public class VectorConfig {
  public static final VectorConfig NOT_ENABLED_CONFIG = new VectorConfig(List.of());

  private final Map<String, VectorColumnDefinition> columnVectorDefinitions;
  private final boolean vectorEnabled;

  /*
   * @param vectorColumnDefinitions - List of vectorColumnDefinitions each with respect to a
   *     column/field
   */
  private VectorConfig(List<VectorColumnDefinition> vectorColumnDefinitions) {
    Objects.requireNonNull(vectorColumnDefinitions, "vectorColumnDefinitions must not be null");

    this.columnVectorDefinitions =
        vectorColumnDefinitions.stream()
            .collect(Collectors.toMap(VectorColumnDefinition::fieldName, Function.identity()));
    this.vectorEnabled = !this.columnVectorDefinitions.isEmpty();
  }

  public static VectorConfig fromColumnDefinitions(
      List<VectorColumnDefinition> vectorColumnDefinitions) {

    if (vectorColumnDefinitions == null || vectorColumnDefinitions.isEmpty()) {
      return NOT_ENABLED_CONFIG;
    }
    return new VectorConfig(vectorColumnDefinitions);
  }

  /** Get table schema object from table metadata */
  public static VectorConfig from(TableMetadata tableMetadata, ObjectMapper objectMapper) {

    Map<String, VectorizeDefinition> vectorizeDefs =
        VectorizeDefinition.from(tableMetadata, objectMapper);
    List<VectorColumnDefinition> columnDefs = new ArrayList<>();

    var vectorColumnMetadata =
        tableMetadata.getColumns().values().stream()
            .filter(column -> column.getType() instanceof VectorType)
            .toList();

    for (ColumnMetadata column : vectorColumnMetadata) {

      // Using internal to match the name of the vector column to the target of the index.
      final Optional<IndexMetadata> columnIndex =
          tableMetadata.getIndexes().values().stream()
              .filter(
                  indexMetadata -> indexMetadata.getTarget().equals(column.getName().asInternal()))
              .findFirst();

      // If there is an index on the column, use the similarity function from the index
      var indexFunction =
          columnIndex.map(
              index -> {
                String similarityFunctionStr =
                    index.getOptions().get(VectorConstants.CQLAnnIndex.SIMILARITY_FUNCTION);
                // if similarity function is set, use it
                SimilarityFunction similarityFunction =
                    similarityFunctionStr != null
                        ? SimilarityFunction.fromCqlIndexingFunction(similarityFunctionStr)
                            .orElseThrow(
                                () ->
                                    SimilarityFunction.getUnknownFunctionException(
                                        similarityFunctionStr))
                        : null;

                String sourceModelStr =
                    index.getOptions().get(VectorConstants.CQLAnnIndex.SOURCE_MODEL);
                EmbeddingSourceModel sourceModel = null;
                if (sourceModelStr != null) {
                  // if similarity function is not set, use the source model to determine it
                  // get with the default model so we can get the similarity function, still may not
                  // find the model
                  // if they have not set it up correctly
                  sourceModel =
                      EmbeddingSourceModel.fromNameOrDefault(sourceModelStr)
                          .orElseThrow(
                              () ->
                                  EmbeddingSourceModel.getUnknownSourceModelException(
                                      sourceModelStr));
                  similarityFunction =
                      similarityFunction == null
                          ? sourceModel.getSimilarityFunction()
                          : similarityFunction;
                }
                return new AbstractMap.SimpleEntry<>(similarityFunction, sourceModel);
              });

      // if no index, or we could not work out the function, default
      var similarityFunction =
          indexFunction.map(Map.Entry::getKey).orElse(SimilarityFunction.COSINE);
      var sourceModel = indexFunction.map(Map.Entry::getValue).orElse(EmbeddingSourceModel.OTHER);
      int dimensions = ((VectorType) column.getType()).getDimensions();

      // NOTE: need to keep the column name as a string in the VectorColumnDefinition
      // because also used by collections
      // OK to not find the vectorize definition, as it is optional
      columnDefs.add(
          new VectorColumnDefinition(
              cqlIdentifierToJsonKey(column.getName()),
              dimensions,
              similarityFunction,
              sourceModel,
              vectorizeDefs.get(column.getName().asInternal())));
    }
    return VectorConfig.fromColumnDefinitions(columnDefs);
  }

  public boolean vectorEnabled() {
    return vectorEnabled;
  }

  public Optional<VectorColumnDefinition> getFirstVectorColumnWithVectorizeDefinition() {
    // HACK - aaron - here so we can get the first definition when processing a table, need to
    // refactor so we
    // not need to know this in the GeneralResource
    return columnVectorDefinitions.values().stream()
        .filter(vectorColumnDefinition -> vectorColumnDefinition.vectorizeDefinition() != null)
        .findFirst();
  }

  public Optional<VectorColumnDefinition> getColumnDefinition(String columnName) {
    return Optional.ofNullable(columnVectorDefinitions.get(columnName));
  }

  public Optional<VectorColumnDefinition> getColumnDefinition(CqlIdentifier identifier) {
    return Optional.ofNullable(columnVectorDefinitions.get(identifier.asInternal()));
  }

  public Optional<VectorizeDefinition> getVectorizeDefinition(CqlIdentifier identifier) {
    return getColumnDefinition(identifier).map(VectorColumnDefinition::vectorizeDefinition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VectorConfig that = (VectorConfig) o;
    return vectorEnabled == that.vectorEnabled
        && Objects.equals(columnVectorDefinitions, that.columnVectorDefinitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnVectorDefinitions, vectorEnabled);
  }
}
