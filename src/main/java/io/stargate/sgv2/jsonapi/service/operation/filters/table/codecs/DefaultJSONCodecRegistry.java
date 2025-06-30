package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Container of {@link JSONCodec} instances that are used to convert Java objects into the objects
 * expected by the CQL driver for specific CQL data types.
 *
 * <p>Use the default instance from {@link JSONCodecRegistries#DEFAULT_REGISTRY}.
 *
 * <p>See {@link #codecToCQL(TableMetadata, CqlIdentifier, Object)} for the main entry point.
 *
 * <p>IMPORTANT: There must be a codec for every CQL data type we want to write to, even if the
 * translation is an identity translation. This is so we know if the translation can happen, and
 * then if it was done correctly with the actual value. See {@link JSONCodec.ToCQL#unsafeIdentity()}
 * for the identity mapping.
 */
public class DefaultJSONCodecRegistry implements JSONCodecRegistry {
  protected static final GenericType<Object> GENERIC_TYPE_OBJECT = GenericType.of(Object.class);

  /**
   * Dummy codec used to convert a JSON null value in CQL. Same "codec" usable for all CQL types,
   * since null CQL driver needs is just plain Java null.
   */
  protected static final JSONCodec<Object, DataType> TO_CQL_NULL_CODEC =
      new JSONCodec<>(
          GENERIC_TYPE_OBJECT,
          /* There's no type for Object so just use something */ DataTypes.BOOLEAN,
          (cqlType, value) -> null,
          null);

  protected final Map<DataType, List<JSONCodec<?, ?>>> codecsByCQLType;

  public DefaultJSONCodecRegistry(List<JSONCodec<?, ?>> codecs) {
    Objects.requireNonNull(codecs, "codecs must not be null");

    codecsByCQLType = new HashMap<>();
    for (JSONCodec<?, ?> codec : codecs) {
      codecsByCQLType.computeIfAbsent(codec.targetCQLType(), k -> new ArrayList<>()).add(codec);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      TableMetadata table, CqlIdentifier column, Object value)
      throws UnknownColumnException, MissingJSONCodecException, ToCQLCodecException {

    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(column, "column must not be null");

    var columnMetadata =
        table.getColumn(column).orElseThrow(() -> new UnknownColumnException(table, column));
    return codecToCQL(table, column, columnMetadata.getType(), value);
  }

  /** {@inheritDoc} */
  @Override
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      TableMetadata table, CqlIdentifier column, DataType toCQLType, Object value)
      throws MissingJSONCodecException, ToCQLCodecException {

    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(column, "column must not be null");
    Objects.requireNonNull(toCQLType, "toCQLType must not be null");

    // Next, simplify later code by handling nulls directly here (but after column lookup)
    if (value == null) {
      return (JSONCodec<JavaT, CqlT>) TO_CQL_NULL_CODEC;
    }
    List<JSONCodec<?, ?>> candidates = codecsByCQLType.get(toCQLType);

    if (candidates != null) {
      // this is a scalar type, so we can just use the first codec
      // And if any found try to match with the incoming Java value
      JSONCodec<JavaT, CqlT> match =
          JSONCodec.unchecked(
              candidates.stream()
                  .filter(codec -> codec.handlesJavaValue(value))
                  .findFirst()
                  .orElse(null));
      if (match == null) {
        // Different exception for this case: CQL type supported but not from given Java type
        // (f.ex, CQL Boolean from Java/JSON number)
        throw new ToCQLCodecException(value, toCQLType, "no codec matching value type");
      }
      return match;
    }

    // CQL collection type and CQL UDT
    // these can return Null if they had no candidates, will throw an exception
    // if they had a candidate but with a type error
    JSONCodec<JavaT, CqlT> complexCodec =
        switch (toCQLType) {
          case ListType lt -> codecToCQL(lt, value);
          case SetType st -> codecToCQL(st, value);
          case MapType mt -> codecToCQL(mt, value);
          case VectorType vt -> codecToCQL(vt, value);
          case UserDefinedType udt -> codecToCQL(udt, value);
          default -> null;
        };

    if (complexCodec != null) {
      return complexCodec;
    }
    throw new MissingJSONCodecException(table, column, toCQLType, value.getClass(), value);
  }

  /** Method to find a codec for the specified CQL List DataType. */
  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(ListType listType, Object value)
      throws ToCQLCodecException {
    // Find codec candidates for the list value as simple primitive type
    List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(listType.getElementType());
    if (valueCodecCandidates != null) {
      // Almost there! But to avoid ClassCastException if input not a JSON Array need this check
      if (!(value instanceof Collection<?>)) {
        throw new ToCQLCodecException(value, listType, "no codec matching value type");
      }
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToCqlListCodec(valueCodecCandidates, listType.getElementType());
    }
    // Find codec for the list element as UDT
    if (listType.getElementType() instanceof UserDefinedType userDefinedType) {
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToCqlListCodec(
              List.of(codecToCQL(userDefinedType, value)), listType.getElementType());
    }
    return null;
  }

  /** Method to find a codec for the specified CQL Set DataType. */
  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(SetType setType, Object value)
      throws ToCQLCodecException {

    // Find codec candidates for the set value as simple primitive type
    List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(setType.getElementType());
    if (valueCodecCandidates != null) {
      // Almost there! But to avoid ClassCastException if input not a JSON Array need this check
      if (!(value instanceof Collection<?>)) {
        throw new ToCQLCodecException(value, setType, "no codec matching value type");
      }
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToCqlSetCodec(valueCodecCandidates, setType.getElementType());
    }
    // Find codec for the set element as UDT
    if (setType.getElementType() instanceof UserDefinedType userDefinedType) {
      return (JSONCodec<JavaT, CqlT>)
          CollectionCodecs.buildToCqlSetCodec(
              List.of(codecToCQL(userDefinedType, value)), setType.getElementType());
    }
    return null;
  }

  /** Method to find a codec for the specified CQL Map DataType. */
  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(MapType mapType, Object value)
      throws ToCQLCodecException {

    List<JSONCodec<?, ?>> keyCodecCandidates = codecsByCQLType.get(mapType.getKeyType());
    List<JSONCodec<?, ?>> valueCodecCandidates = codecsByCQLType.get(mapType.getValueType());
    // Find codec for the map value as UDT
    if (valueCodecCandidates == null) {
      if (mapType.getValueType() instanceof UserDefinedType userDefinedType) {
        valueCodecCandidates = List.of(codecToCQL(userDefinedType, value));
      }
    }
    if (keyCodecCandidates != null && valueCodecCandidates != null) {
      // Almost there! But to avoid ClassCastException if input not a JSON Array need this check
      if (!(value instanceof Map<?, ?>)) {
        throw new ToCQLCodecException(value, mapType, "no codec matching value type");
      }
      return (JSONCodec<JavaT, CqlT>)
          MapCodecs.buildToCqlMapCodec(
              keyCodecCandidates,
              valueCodecCandidates,
              mapType.getKeyType(),
              mapType.getValueType());
    }
    return null;
  }

  /** Method to find a codec for the specified CQL UDT. */
  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(
      UserDefinedType userDefinedType, Object value) throws ToCQLCodecException {

    Map<CqlIdentifier, List<JSONCodec<?, ?>>> fieldsCodecCandidates = new HashMap<>();
    Map<CqlIdentifier, DataType> fieldTypes = new HashMap<>();
    // There is no map setup in driver to represent the UDT fields name and types.
    // So have to iterate by using indexes for two lists below.
    for (int i = 0; i < userDefinedType.getFieldNames().size(); i++) {
      var fieldIdentifier = userDefinedType.getFieldNames().get(i);
      var fieldCqlType = userDefinedType.getFieldTypes().get(i);

      List<JSONCodec<?, ?>> fieldCodecCandidates = codecsByCQLType.get(fieldCqlType);
      if (fieldCodecCandidates == null) {
        throw new ToCQLCodecException(
            value,
            userDefinedType,
            "no codec matching field %s, %s"
                .formatted(fieldIdentifier.asInternal(), fieldCqlType.toString()));
      }
      fieldsCodecCandidates.put(fieldIdentifier, fieldCodecCandidates);
      fieldTypes.put(fieldIdentifier, fieldCqlType);
    }

    return (JSONCodec<JavaT, CqlT>)
        UDTCodecs.buildToCqlUdtCodec(fieldsCodecCandidates, fieldTypes, userDefinedType);
  }

  /** Method to find a codec for the specified CQL Vector DataType. */
  protected <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToCQL(VectorType vectorType, Object value)
      throws ToCQLCodecException {

    // Only Float<Vector> supported for now
    if (!vectorType.getElementType().equals(DataTypes.FLOAT)) {
      throw new ToCQLCodecException(value, vectorType, "only Vector<Float> supported");
    }
    if (value instanceof Collection<?>) {
      return VectorCodecs.arrayToCQLFloatArrayCodec(vectorType);
    }
    if (value instanceof EJSONWrapper) {
      return VectorCodecs.binaryToCQLFloatArrayCodec(vectorType);
    }
    if (value instanceof float[]) {
      return VectorCodecs.floatArrayToCQLFloatArrayCodec(vectorType);
    }
    throw new ToCQLCodecException(value, vectorType, "no codec matching value type");
  }

  /**
   * Method to find a codec for the specified CQL Type, converting from Java to JSON. Notice this
   * method has recursive calls to itself, so it can handle subTypes.
   *
   * <ul>
   *   <li>if the `fromCQLType` is a Set of TEXT, it will call itself to get the codec for the TEXT.
   *   <li>if the `fromCQLType` is a List of UDT, it will call itself to get the codec for the UDT.
   *   <li>if the `fromCQLType` is a Map of TEXT to UDT, it will call itself to get the codec for
   *       TEXT and UDT.
   * </ul>
   *
   * @param fromCQLType
   * @return Codec to use for conversion, or `null` if none found.
   */
  @Override
  public <JavaT, CqlT> JSONCodec<JavaT, CqlT> codecToJSON(DataType fromCQLType) {

    // check if fromCQLType a primitive type
    List<JSONCodec<?, ?>> candidates = codecsByCQLType.get(fromCQLType);
    if (candidates != null) {
      // Scalar type codecs found: use first one (all have same to-json handling)
      // Can choose any one of codecs (since to-JSON is same for all); but must get one
      return JSONCodec.unchecked(candidates.getFirst());
    }

    if (fromCQLType instanceof ListType lt) {
      DataType elementType = lt.getElementType();
      JSONCodec<?, ?> elementCodec = codecToJSON(elementType);
      if (elementCodec != null) {
        return (JSONCodec<JavaT, CqlT>) CollectionCodecs.buildToJsonListCodec(elementCodec);
      }
      return null; // caller handles missing codec
    }

    if (fromCQLType instanceof SetType st) {
      DataType elementType = st.getElementType();
      JSONCodec<?, ?> elementCodec = codecToJSON(elementType);
      if (elementCodec != null) {
        return (JSONCodec<JavaT, CqlT>) CollectionCodecs.buildToJsonSetCodec(elementCodec);
      }
      return null; // caller handles missing codec
    }

    if (fromCQLType instanceof MapType mt) {
      final DataType keyType = mt.getKeyType();
      JSONCodec<?, ?> keyCodec = codecToJSON(keyType);
      JSONCodec<?, ?> valueCodec = codecToJSON(mt.getValueType());
      if (keyCodec == null || valueCodec == null) {
        return null; // so caller reports problem
      }
      return (JSONCodec<JavaT, CqlT>) MapCodecs.buildToJsonMapCodec(keyType, keyCodec, valueCodec);
    }

    if (fromCQLType instanceof VectorType vt) {
      // Only Float<Vector> supported for now
      if (vt.getElementType().equals(DataTypes.FLOAT)) {
        return VectorCodecs.toJSONFloatVectorCodec(vt);
      }
      return null;
    }

    if (fromCQLType instanceof UserDefinedType userDefinedType) {
      Map<CqlIdentifier, JSONCodec<?, ?>> fieldCodecs = new HashMap<>();
      // There is no map setup in driver to represent the UDT fields name and types.
      // So have to iterate by using indexes for two lists below.
      for (int i = 0; i < userDefinedType.getFieldNames().size(); i++) {
        var fieldIdentifier = userDefinedType.getFieldNames().get(i);
        var fieldCqlType = userDefinedType.getFieldTypes().get(i);
        JSONCodec<?, ?> fieldCodec = codecToJSON(fieldCqlType);
        // if no codec candidates found for ANY field, return null
        if (fieldCodec == null) {
          return null;
        }
        fieldCodecs.put(fieldIdentifier, fieldCodec);
      }
      return (JSONCodec<JavaT, CqlT>) UDTCodecs.buildToJsonUdtCodec(fieldCodecs, userDefinedType);
    }

    return null;
  }
}
