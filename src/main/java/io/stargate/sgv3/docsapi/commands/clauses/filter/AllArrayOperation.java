package io.stargate.sgv3.docsapi.commands.clauses.filter;

public class AllArrayOperation extends ArrayOperation {
  public final ArrayOperationName operator = ArrayOperationName.ALL;
  public JsonLiteralList rhsOperand;
}
