package io.stargate.sgv2.jsonapi.exception.checked;

/** Thrown when asked to parse the name of a API data type for a table column. */
public class UnknownApiDataType extends CheckedApiException {

  private final String typeName;

  public UnknownApiDataType(String typeName) {
    super(String.format("Unknown API Table data type: %s", typeName));
    this.typeName = typeName;
  }

  public String typeName() {
    return typeName;
  }
}
