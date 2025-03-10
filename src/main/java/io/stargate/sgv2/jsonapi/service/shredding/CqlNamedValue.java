package io.stargate.sgv2.jsonapi.service.shredding;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingAction;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodec;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A value that can be sent to the CQL Driver or has come from it.
 *
 * <p>The value must be of the correct type for the column described by the {@link ColumnMetadata},
 * this means when handling values passed in they have gone through the {@link JSONCodec#toCQL()}
 * method.
 */
public class CqlNamedValue extends NamedValue<CqlIdentifier, Object, JsonNamedValue> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CqlNamedValue.class);

  public static final Comparator<CqlNamedValue> NAME_COMPARATOR =
      Comparator.comparing(CqlNamedValue::name, CQL_IDENTIFIER_COMPARATOR);

  private final JSONCodecRegistry codecRegistry;
  private final ErrorStrategy<? extends RequestException> errorStrategy;

  public CqlNamedValue(
      CqlIdentifier name,
      JSONCodecRegistry codecRegistry,
      ErrorStrategy<? extends RequestException> errorStrategy) {
    super(name);

    this.codecRegistry = Objects.requireNonNull(codecRegistry, "codecRegistry must not be null");
    this.errorStrategy = Objects.requireNonNull(errorStrategy, "errorStrategy must not be null");
  }

  @Override
  protected ApiColumnDef bindToColumn(TableSchemaObject tableSchemaObject) {
    var apiColumnDef = tableSchemaObject.apiTableDef().allColumns().get(name());
    if (apiColumnDef == null) {
      setErrorCode(NamedValueState.BIND_ERROR, errorStrategy.codeForUnknownColumn());
    }
    return apiColumnDef;
  }

  @Override
  protected DecodeResult<Object> decodeValue(JsonNamedValue rawValue) {

    // First - check if we want to defer the decode because we need to generate a vector
    // need a vector column, with vectorize def, and the raw value to be a string
    if ((apiColumnDef().type() instanceof ApiVectorType vectorType)
        && (rawValue.value().type().equals(JsonType.STRING))) {

      if ((vectorType.getVectorizeDefinition() == null)) {
        setErrorCode(NamedValueState.PREPARE_ERROR, errorStrategy.codeForMissingVectorize());
        // ok to return null here, we have set the error code
        return null;
      } else {
        return maybeVectorize(rawValue);
      }
    }

    // not deferring, so we need to push the value through the codecs to get what to send to CQL
    return decodeToCQL(rawValue.value().value());
  }

  private DecodeResult<Object> decodeToCQL(Object rawValue) {

    try {
      var codec = codecRegistry.codecToCQL(schemaObject().tableMetadata(), name(), rawValue);
      return new DecodeResult<>(codec.toCQL(rawValue), null);

    } catch (UnknownColumnException e) {
      // this should not happen, we checked above but the codecs are written to be very safe and
      // will check and throw
      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
    } catch (MissingJSONCodecException e) {
      setErrorCode(NamedValueState.PREPARE_ERROR, errorStrategy.codeForMissingCodec());
    } catch (ToCQLCodecException e) {
      setErrorCode(NamedValueState.PREPARE_ERROR, errorStrategy.codeForCodecError());
    }

    // ok to return null here, we have set the error code
    return null;
  }

  private DecodeResult<Object> maybeVectorize(JsonNamedValue rawValue) {

    var vectorizeText = rawValue.value().value().toString();
    // we have a vectorize text, if the text is empty we simply set the value of the vector to be
    // null
    if (vectorizeText.isBlank()) {
      return new DecodeResult<>(null, null);
    }

    // vectorize this sucker
    // we give the value generator a consumer so we can prepare the value when we get it back.
    return new DecodeResult<>(
        null,
        new EmbeddingAction(
            vectorizeText,
            apiColumnDef(),
            this::consumeEmbeddingSuccess,
            this::consumeEmbeddingFailure));
  }

  /**
   * Consumer for when we get the vector we needed to generate.
   *
   * <p>The generator will have checked we got what the column needs, we need to run it through the
   * codec to get the value to send to CQL.
   *
   * @param vector
   */
  private void consumeEmbeddingSuccess(float[] vector) {

    var decoded = decodeToCQL(vector);
    if (decoded == null) {
      // when the NamedValues is build by the CqlNamedValue factory it will run the checks from the
      // error strategy, so we need to run them here because this is post factory generation
      // Not easy to collect all the NamedValues that could be receiving a vector so need to it in
      // each
      errorStrategy.allChecks(schemaObject(), new CqlNamedValueContainer(List.of(this)));

      // Sanity check
      throw new IllegalStateException(
          "NamedValue: decodeToCQL returned null, and the error strategy did not throw for name: "
              + name());
    }

    // use the super class so state is set correctly
    setDecodedValue(decoded.value());
  }

  private void consumeEmbeddingFailure(RuntimeException exception) {
    // todo: it would be good to capture the code, need to track and expose the raw errorInstance to
    // get that.
    // and change the generic base from Request to Server fo
    setErrorCode(NamedValueState.GENERATOR_ERROR, null);
  }

  public interface ErrorStrategy<T extends RequestException> {

    ErrorCode<T> codeForUnknownColumn();

    ErrorCode<T> codeForMissingVectorize();

    ErrorCode<T> codeForMissingCodec();

    ErrorCode<T> codeForCodecError();

    void allChecks(TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns);

    default void checkUnknownColumns(
        TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {

      var unknownColumns =
          allColumns.values().stream()
              .filter(
                  cqlNamedValue ->
                      cqlNamedValue.state().equals(NamedValue.NamedValueState.BIND_ERROR))
              .filter(cqlNamedValue -> cqlNamedValue.errorCode().equals(codeForUnknownColumn()))
              .sorted(CqlNamedValue.NAME_COMPARATOR)
              .toList();

      if (!unknownColumns.isEmpty()) {
        throw codeForUnknownColumn()
            .get(
                errVars(
                    tableSchemaObject,
                    map -> {
                      map.put(
                          "allColumns",
                          errFmtColumnMetadata(
                              tableSchemaObject.tableMetadata().getColumns().values()));
                      map.put("unknownColumns", errFmtCqlNamedValue(unknownColumns));
                    }));
      }
    }

    default void checkMissingCodec(
        TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {

      var missingCodecs =
          allColumns.values().stream()
              .filter(
                  cqlNamedValue ->
                      cqlNamedValue.state().equals(NamedValue.NamedValueState.PREPARE_ERROR))
              .filter(cqlNamedValue -> cqlNamedValue.errorCode().equals(codeForMissingCodec()))
              .sorted(CqlNamedValue.NAME_COMPARATOR)
              .toList();

      if (!missingCodecs.isEmpty()) {
        throw codeForMissingCodec()
            .get(
                errVars(
                    tableSchemaObject,
                    map -> {
                      map.put(
                          "allColumns",
                          errFmtColumnMetadata(
                              tableSchemaObject.tableMetadata().getColumns().values()));
                      map.put("unsupportedColumns", errFmtCqlNamedValue(missingCodecs));
                    }));
      }
    }

    default void checkCodecError(
        TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {

      var codecErrors =
          allColumns.values().stream()
              .filter(
                  cqlNamedValue ->
                      cqlNamedValue.state().equals(NamedValue.NamedValueState.PREPARE_ERROR))
              .filter(cqlNamedValue -> cqlNamedValue.errorCode().equals(codeForCodecError()))
              .sorted(CqlNamedValue.NAME_COMPARATOR)
              .toList();

      if (!codecErrors.isEmpty()) {
        throw codeForCodecError()
            .get(
                errVars(
                    tableSchemaObject,
                    map -> {
                      map.put(
                          "allColumns",
                          errFmtColumnMetadata(
                              tableSchemaObject.tableMetadata().getColumns().values()));
                      map.put("invalidColumns", errFmtCqlNamedValue(codecErrors));
                    }));
      }
    }

    default void checkMissingVectorize(
        TableSchemaObject tableSchemaObject, CqlNamedValueContainer allColumns) {

      var missingVectorizeColumns =
          allColumns.values().stream()
              .filter(
                  cqlNamedValue ->
                      cqlNamedValue.state().equals(NamedValue.NamedValueState.PREPARE_ERROR))
              .filter(cqlNamedValue -> cqlNamedValue.errorCode().equals(codeForMissingVectorize()))
              .sorted(CqlNamedValue.NAME_COMPARATOR)
              .toList();

      if (!missingVectorizeColumns.isEmpty()) {
        var vectorizeColumns =
            tableSchemaObject
                .apiTableDef()
                .allColumns()
                .filterByApiTypeNameToList(ApiTypeName.VECTOR)
                .stream()
                .filter(
                    columnDef ->
                        ((ApiVectorType) columnDef.type()).getVectorizeDefinition() != null)
                .sorted(ApiColumnDef.NAME_COMPARATOR)
                .toList();

        throw codeForMissingVectorize()
            .get(
                errVars(
                    tableSchemaObject,
                    map -> {
                      map.put("validVectorizeColumns", errFmtApiColumnDef(vectorizeColumns));
                      map.put(
                          "invalidVectorizeColumns", errFmtCqlNamedValue(missingVectorizeColumns));
                    }));
      }
    }
  }
}
