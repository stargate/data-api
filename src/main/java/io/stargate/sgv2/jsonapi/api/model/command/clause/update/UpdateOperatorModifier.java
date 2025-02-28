package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

public enum UpdateOperatorModifier {
  EACH("$each"),
  POSITION("$position");

  private final String modifier;

  UpdateOperatorModifier(String modifier) {
    this.modifier = modifier;
  }

  public String getModifier() {
    return modifier;
  }
}
