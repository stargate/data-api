package io.stargate.sgv3.docsapi.commands.clauses.update;

public abstract class FieldUpdateOperation extends UpdateClauseOperation {

  private final FieldUpdateOperator operator;

  protected FieldUpdateOperation(FieldUpdateOperator operator) {
    this.operator = operator;
  }

  @Override
  public String operatorName() {
    return operator.name;
  }

  public enum FieldUpdateOperator {
    SET("$set");

    public final String name;

    private FieldUpdateOperator(String name) {
      this.name = name;
    }
  }
}
