package io.stargate.sgv3.docsapi.commands.clauses.update;

public abstract class ArrayUpdateOperation extends UpdateClauseOperation {

  private ArrayUpdateOperator operator;

  protected ArrayUpdateOperation(ArrayUpdateOperator operator) {
    this.operator = operator;
  }

  @Override
  public String operatorName() {
    return operator.name;
  }

  public enum ArrayUpdateOperator {
    PUSH("$push");

    public final String name;

    private ArrayUpdateOperator(String name) {
      this.name = name;
    }
  }
}
