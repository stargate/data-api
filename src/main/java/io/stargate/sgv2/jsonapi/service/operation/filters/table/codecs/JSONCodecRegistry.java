package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;

/** TODO: DOCS */
public interface JSONCodecRegistry {

  /**
   * Returns a codec that can convert a Java object into the object expected by the CQL driver for a
   * specific CQL data type.
   *
   * <p>
   *
   * @param table {@link TableMetadata} to find the column definition in
   * @param column {@link CqlIdentifier} for the column we want to get the codec for.
   * @param value The value to be written to the column
   * @param <JavaT> Type of the Java object we want to convert.
   * @param <CqlT> Type fo the Java object the CQL driver expects.
   * @return The {@link JSONCodec} that can convert the value to the expected type for the column,
   *     or an exception if the codec cannot be found.
   * @throws UnknownColumnException If the column is not found in the table.
   * @throws MissingJSONCodecException If no codec is found for the column and type of the value.
   * @throws ToCQLCodecException If there is a codec for CQL type, but not one for converting from
   *     the Java value type
   */
  <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      TableMetadata table, CqlIdentifier column, Object value)
      throws UnknownColumnException, MissingJSONCodecException, ToCQLCodecException;

  default <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(
      TableMetadata table, ColumnMetadata column) throws MissingJSONCodecException {
    // compiler telling me we need to use the unchecked assignment again like the codecFor does
    return codecToJSON(column.getType());
  }

  /**
   * Method to find a codec for the specified CQL Type, converting from Java to JSON
   *
   * @param fromCQLType
   * @return Codec to use for conversion, or `null` if none found.
   */
  <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(DataType fromCQLType)
      throws MissingJSONCodecException;
}
