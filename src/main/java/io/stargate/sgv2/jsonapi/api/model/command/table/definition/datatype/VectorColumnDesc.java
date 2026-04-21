package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescSource;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.Objects;

/** Column type for {@link ApiVectorType} */
public class VectorColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Float will be default type for vector
  private final ColumnDesc valueType;
  private final Integer dimension;
  private final VectorizeConfig vectorizeConfig;

  public VectorColumnDesc(
      SchemaDescSource schemaDescSource, Integer dimension, VectorizeConfig vectorizeConfig) {
    this(schemaDescSource, dimension, vectorizeConfig, null);
  }

  public VectorColumnDesc(
      SchemaDescSource schemaDescSource,
      Integer dimension,
      VectorizeConfig vectorizeConfig,
      ApiSupportDesc apiSupportDesc) {
    super(schemaDescSource, ApiTypeName.VECTOR, apiSupportDesc);

    this.valueType = PrimitiveColumnDesc.FLOAT;
    this.dimension = dimension;
    this.vectorizeConfig = vectorizeConfig;
  }

  public VectorizeConfig getVectorizeConfig() {
    return vectorizeConfig;
  }

  public Integer getDimension() {
    return dimension;
  }

  public ColumnDesc valueType() {
    return valueType;
  }

  // Needed for testing
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var vectDesc = (VectorColumnDesc) o;
    return Objects.equals(dimension, vectDesc.dimension)
        && Objects.equals(valueType, vectDesc.valueType)
        && Objects.equals(vectorizeConfig, vectDesc.vectorizeConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType, dimension, vectorizeConfig);
  }

  /**
   * Factory to create a {@link VectorColumnDesc} from JSON representing the type
   *
   * <p>...
   */
  public static class FromJsonFactory extends ColumnDescFromJsonFactory<VectorColumnDesc> {
    FromJsonFactory() {}

    @Override
    public VectorColumnDesc create(
        String columnName,
        SchemaDescSource schemaDescSource,
        JsonParser jsonParser,
        JsonNode columnDescNode)
        throws JsonProcessingException {

      // if the nodes are missing, the jackson MissingNode will be returned and has "" and 0 for
      // defaults. but to get a decent error message get the dimension as a string
      // arguable that a non integer is a JSON mapping error, but we will handle it as an
      // unsupported
      // dimension value
      var dimensionString = columnDescNode.path(TableDescConstants.ColumnDesc.DIMENSION).asText();

      // call to readTreeAsValue will throw JacksonException, this should be if the databinding is
      // not correct, e.g. if there is a missing field, or the field is not the correct type
      // ok to let this out
      var serviceNode = columnDescNode.path(TableDescConstants.ColumnDesc.SERVICE);
      VectorizeConfig vectorConfig =
          serviceNode.isMissingNode()
              ? null
              : OBJECT_MAPPER.treeToValue(serviceNode, VectorizeConfig.class);

      // if the string is null/empty/blank, we think the user doesn't provide the dimension
      // we will check later if the null dimension is allowed
      // when we create the ApiVectorType
      if (dimensionString == null || dimensionString.isBlank()) {
        return new VectorColumnDesc(schemaDescSource, null, vectorConfig);
      }

      // dimensionString cannot be non-empty string or the value is negative
      Integer dimension = null;
      try {
        dimension = Integer.parseInt(dimensionString);
      } catch (NumberFormatException e) {
        // we got something that was not null or blank, and not a number so some sort of non numeric
        // string.
        // We try to throw the invalid config errors in the type factories, but in this case the
        // problem
        // is we hae a string, but it should be a number so OK.
        throw SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION.get(
            "unsupportedValue", dimensionString);
      }

      return new VectorColumnDesc(schemaDescSource, dimension, vectorConfig);
    }
  }
}
