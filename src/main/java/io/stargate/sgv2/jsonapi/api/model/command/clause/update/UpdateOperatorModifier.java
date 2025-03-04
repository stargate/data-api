package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

public enum UpdateOperatorModifier {
  EACH("$each"),
  POSITION("$position");

  private final String apiName;

  UpdateOperatorModifier(String apiName) {
    this.apiName = apiName;
  }

  public String apiName() {
    return apiName;
  }
}
