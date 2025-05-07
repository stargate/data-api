package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.api.model.command.table.MapSetListComponent;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.*;
import javax.annotation.Nullable;
import org.bson.types.ObjectId;

/**
 * This object represents the operator and rhs operand of a filter clause
 *
 * @param operator Filter condition operator
 * @param operand Filter clause operand
 */
public record ValueComparisonOperation<T>(
    @NotNull(message = "operator cannot be null") FilterOperator operator,
    @NotNull(message = "operand cannot be null") JsonLiteral<T> operand,
    @Nullable MapSetListComponent mapSetListComponent)
    implements FilterOperation<T> {

  /**
   * Build a {@link ValueComparisonOperation} from the FilterOperator and operand node value from
   * request filter json. It is not against a table map/set/list column, so mapSetListComponent is
   * null.
   */
  public static ValueComparisonOperation<?> build(
      @NotNull(message = "operator cannot be null") FilterOperator operator,
      @NotNull(message = "operand cannot be null") Object operandValue) {
    return new ValueComparisonOperation<>(operator, getLiteral(operandValue), null);
  }

  /**
   * Build a {@link ValueComparisonOperation} from the FilterOperator and operand node value for a
   * specific map/set/list component.
   */
  public static ValueComparisonOperation<?> build(
      @NotNull(message = "operator cannot be null") FilterOperator operator,
      @NotNull(message = "operand cannot be null") Object operandValue,
      @NotNull(message = "mapSetListComponent cannot be null")
          MapSetListComponent mapSetListComponent) {
    return new ValueComparisonOperation<>(operator, getLiteral(operandValue), mapSetListComponent);
  }

  /** {@inheritDoc} */
  @Override
  public boolean match(
      Set<? extends FilterOperator> operators, JsonType type, boolean toMatchMapSetListComponent) {
    // No need to check specific mapSetListComponent for table filtering feature.
    // Only checks if current FilterOperation has mapSetListComponent or not.
    if (toMatchMapSetListComponent && mapSetListComponent == null) {
      return false;
    }
    if (!toMatchMapSetListComponent && mapSetListComponent != null) {
      return false;
    }
    return operators.contains(operator) && type.equals(operand.type());
  }

  /**
   * Create Typed JsonLiteral object for the plain object from request filter json body.
   *
   * @param value object came in the request.
   * @return {@link JsonLiteral}.
   */
  private static JsonLiteral<?> getLiteral(Object value) {
    if (value == null) {
      return new JsonLiteral<>(null, JsonType.NULL);
    }
    if (value instanceof DocumentId) {
      return new JsonLiteral<>((DocumentId) value, JsonType.DOCUMENT_ID);
    }
    if (value instanceof BigDecimal) {
      return new JsonLiteral<>((BigDecimal) value, JsonType.NUMBER);
    }
    if (value instanceof Boolean) {
      return new JsonLiteral<>((Boolean) value, JsonType.BOOLEAN);
    }
    if (value instanceof Date) {
      return new JsonLiteral<>((Date) value, JsonType.DATE);
    }
    if (value instanceof String) {
      return new JsonLiteral<>((String) value, JsonType.STRING);
    }
    if (value instanceof List) {
      return new JsonLiteral<>((List<Object>) value, JsonType.ARRAY);
    }
    if (value instanceof Map map) {
      return new JsonLiteral<>(map, JsonType.SUB_DOC);
    }
    if (value instanceof UUID || value instanceof ObjectId) {
      return new JsonLiteral<>(value.toString(), JsonType.STRING);
    }
    if (value instanceof byte[] bytes) {
      return new JsonLiteral<>(bytes, JsonType.EJSON_WRAPPER);
    }
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "Unsupported filter value type `%s`", value.getClass().getName());
  }
}
