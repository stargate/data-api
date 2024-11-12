package io.stargate.sgv2.jsonapi.exception.checked;

import java.util.Objects;

public class UnknownCqlIndexFunctionException extends CheckedApiException {

  private final String functionName;

  public UnknownCqlIndexFunctionException(String functionName) {
    super(
        "Unknown CQL index function name: "
            + Objects.requireNonNull(functionName, "functionName must not be null"));
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }
}
