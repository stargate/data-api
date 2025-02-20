package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

import java.util.Objects;

/**
 * Abstract idea of a value that has a name.
 *
 * <p>In the API this could be:
 *
 * <ul>
 *   <li>{@link JsonNamedValue} value shredded from a document with the Java value returned from
 *       Jackson, or a value ready to be used to create a JSON document.
 *   <li>{@link CqlNamedValue} that is ready to be passed to the driver for inserting or filtering
 *       or, or a value read from the driver
 * </ul>
 *
 * @param <NameT> The type of the name of the value
 * @param <ValueT> The type of the value
 */
public abstract class NamedValue<NameT, ValueT, RawValueT> implements PrettyPrintable {

  public enum NamedValueState{
    INITIAL(false, false),
    BOUND(false, false),
    DEFERRED(false, false),
    PREPARED(true, false),
    BIND_ERROR(false, true),
    PREPARE_ERROR(false, true),
    GENERATOR_ERROR(false, true);

    public final boolean isTerminal;
    public final boolean isError;

    NamedValueState(boolean isTerminal, boolean isError) {
      this.isTerminal = isTerminal;
      this.isError = isError;
    }
  }

  private NamedValueState state;
  private TableSchemaObject tableSchemaObject;
  private ApiColumnDef columnDef;
  // Nullable error code if there is an error when binding, or preparing the named value
  // use a code so multiple columns getting the same error can be grouped
  private ErrorCode<? extends RequestException> errorCode;

  protected final NameT name;
  protected ValueT value;
  protected ValueGenerator valueGenerator;

  protected NamedValue(NameT name) {
    this.name = name;
    this.state = NamedValueState.INITIAL;
  }

  // ===============================================================================================
  // Public API for working with a NamedValue
  // ===============================================================================================

  /**
   * Use the name given for the NV to bind to the schema
   * @param tableSchemaObject
   */
  public boolean bind(TableSchemaObject tableSchemaObject) {
    this.tableSchemaObject = Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");

    // can only bind once
    checkIsState(NamedValueState.INITIAL, "bind()");

    // subclass needs to know how to use the NameT to get a column
    columnDef = bindToColumn();

    // if we do not get a column that is OK, the subclass must has set the state to BIND_ERROR because it
    // could not find the column and it may be setting other sublcass specific info
    if (columnDef == null) {
      checkIsState(NamedValueState.BIND_ERROR, "bindToColumn() returned null" );
      return false;
    }
    setState(NamedValueState.BOUND);
    return true;
  }

  /**
   * Called to prepare the value for the named value and store it in the named value
   *
   * @param rawValue
   */
  public boolean prepare(RawValueT rawValue){

    checkIsState(NamedValueState.BOUND, "prepare()");
    var decodeResult = decodeValue(rawValue);

    if (state().isError) {
      return false;
    }

    if (decodeResult == null) {
      throw new IllegalStateException("NamedValue: decodeResult returned null for name: " + name);
    }

    if (decodeResult.valueGenerator() != null) {
      valueGenerator = decodeResult.valueGenerator();
      setState(NamedValueState.DEFERRED);
    }
    else {
      setDecodedValue(decodeResult.value());
    }
    return true;
  }

  // ===============================================================================================
  // Subclass methods and implementation it may override
  // ===============================================================================================

  /**
   * Called to get a reference to the column the value is bound to.
   * @return
   */
  protected abstract ApiColumnDef bindToColumn();

  /**
   * Called to take the raw value and decode into what we need for this context.
   * e.g. from JSON to Java or from Java to CQL
   * @param rawValue
   * @return
   */
  protected abstract DecodeResult<ValueT> decodeValue(RawValueT rawValue);


  /**
   * Called to store the decoded value in the named value, is a seperate method so that
   * generated values can call this directly without going through the prepare() method
   * @param value
   */
  protected void setDecodedValue(ValueT value) {

    // TODO: this can be in either BOUND or DEFERRED state, need a mulit state check

    // the value can be null
    this.value = value;
    setState(NamedValueState.PREPARED);
  }

  protected TableSchemaObject schemaObject() {
    checkIsState(NamedValueState.BOUND, "schemaObject()");
    return tableSchemaObject;
  }

  public ApiColumnDef apiColumnDef() {
    checkIsState(NamedValueState.BOUND, "columnDef()");
    return columnDef;
  }

  // ===============================================================================================
  // Properties  - public and protected
  // ===============================================================================================

  public NameT name() {
    return name;
  }

  public ValueT value() {
    checkIsState(NamedValueState.PREPARED, "value()");
    return value;
  }

  public ValueGenerator valueGenerator() {
    checkIsState(NamedValueState.DEFERRED, "valueGenerator()");
    return valueGenerator;
  }

  public NamedValueState state() {
    return state;
  }

  protected void setState(NamedValueState state) {
    this.state = state;
  }

  protected void checkIsState(NamedValueState expectedState, String context) {
    if (!state().equals(expectedState)) {
      throw new IllegalStateException(String.format("NamedValue: not in expected state for %s, name: %s, expected: %s, actual: %s", context, name, expectedState, state()));
    }
  }

  protected void checkNotErrorState(String context) {
    if (state().isError) {
      throw new IllegalStateException(String.format("NamedValue: should not be in error state for %s, name: %s, state: %s", context, name, state()));
    }
  }

  public ErrorCode<? extends RequestException> errorCode() {
    return errorCode;
  }

  protected void setErrorCode(NamedValueState errorState, ErrorCode<? extends RequestException> errorCode) {
    checkNotErrorState("setError()");

    this.errorCode = errorCode;
    setState(errorState);
  }


  protected record DecodeResult<ValueT>(ValueT value, ValueGenerator valueGenerator) {}

  @Override
  public String toString() {
    return toString(false);
  }

  @Override
  public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    prettyToStringBuilder.append("name", name).append("value", value);
    return prettyToStringBuilder;
  }
}
