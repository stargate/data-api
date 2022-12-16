package io.stargate.sgv3.docsapi.commands.clauses.filter;

/**
 * <filter-element-operation> ::= <filter-element-operation-exists> THIS ->
 * <filter-element-operation-exists> ::= $exists <filter-element-operation-exists-operand>
 * <filter-element-operation-exists-operand> ::= true | false
 */
public class ExistsElementOperation extends ElementOperation {
  public final ElementOperationName operator = ElementOperationName.EXISTS;
  public boolean rhsOperand;
}
