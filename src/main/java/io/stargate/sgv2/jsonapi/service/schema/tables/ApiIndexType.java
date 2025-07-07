package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlIndexException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What type of index the API views a CQL index as.
 *
 * <p>The value of the index type maps to the API command that can be used to create the index.
 */
public enum ApiIndexType {
  /** Index on a scalar column, or collection */
  REGULAR(Constants.REGULAR),
  /** Index that uses a text analyzer on ascii or text columns */
  TEXT(Constants.TEXT),
  /** Index on a vector column */
  VECTOR(Constants.VECTOR);

  /** Constants so the public HTTP API objects can use the same values. */
  public interface Constants {
    String REGULAR = "regular";
    String TEXT = "text";
    String VECTOR = "vector";
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ApiIndexType.class);

  private final String apiName;

  private static final Map<String, ApiIndexType> BY_API_NAME;

  static {
    var map = new HashMap<String, ApiIndexType>();
    for (ApiIndexType type : ApiIndexType.values()) {
      map.put(type.apiName.toLowerCase(), type);
    }
    BY_API_NAME = Map.copyOf(map);
  }

  ApiIndexType(String apiName) {
    this.apiName = Objects.requireNonNull(apiName, "apiName must not be null");
  }

  /**
   * Gets the name to use when identifying this type of index in the public API.
   *
   * @return
   */
  public String apiName() {
    return apiName;
  }

  /**
   * Get the ApiIndexType by the API name.
   *
   * @param apiName The name of the index type from the public API. If the name is not found, it
   *     will return the defaultType.
   * @return The ApiIndexType or null if not found
   */
  public static ApiIndexType fromApiName(String apiName) {
    return apiName == null ? null : BY_API_NAME.get(apiName.toLowerCase());
  }

  /**
   * Determine the ApiIndexType from the CQL index metadata.
   *
   * @param apiColumnDef The Column the index is on.
   * @param indexTarget The parsed information about the target and function the index uses.
   * @param indexMetadata CQL driver metadata for the index.
   * @return The ApiIndexType for the index, if it cannot be determined it will throw an exception.
   * @throws UnsupportedCqlIndexException Unable to determine or unsupported index type.
   */
  public static ApiIndexType fromCql(
      ApiColumnDef apiColumnDef, CQLSAIIndex.IndexTarget indexTarget, IndexMetadata indexMetadata)
      throws UnsupportedCqlIndexException {

    // TODO: XXX: AARON - Possible bug here, unsupported types do not have a type
    final ApiDataType columnType = apiColumnDef.type();

    // Let's start with Text (aka Lexical, or Analyzed) indexes: only for TEXT or ASCII columns
    // and with a text analyzer defined in the index options.
    switch (columnType.typeName()) {
      case ASCII, TEXT -> {
        String analyzerDef =
            indexMetadata.getOptions().get(TableDescConstants.TextIndexCQLOptions.OPTION_ANALYZER);
        if (analyzerDef != null && !analyzerDef.isBlank()) {
          return TEXT;
        }
      }
    }

    // If there is no function on the indexTarget, and the column is a scalar, then it is a regular
    // index on an int, text, etc
    if (indexTarget.indexFunction() == null && columnType.isPrimitive()) {
      return REGULAR;
    }

    // if the target column is a vector, it can only be a vector index, we will let building the
    // index check the options.
    // NOTE: check this before the container check, as a vector type is a container type
    if (indexTarget.indexFunction() == null && columnType.typeName() == ApiTypeName.VECTOR) {
      return VECTOR;
    }

    // if the target column is a collection, it can only be a collection index,
    // collection indexes must have a function
    if (indexTarget.indexFunction() != null
        && columnType.isContainer()
        && columnType.typeName() != ApiTypeName.VECTOR) {
      return REGULAR;
    }

    throw new UnsupportedCqlIndexException(
        "Unsupported index:%s on field: %s".formatted(indexMetadata.getName(), apiColumnDef.name()),
        indexMetadata);
  }
}
