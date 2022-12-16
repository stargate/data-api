package io.stargate.sgv3.docsapi.commands.clauses.filter;

public class SizeArrayOperation extends ArrayOperation {
  public final ArrayOperationName operator = ArrayOperationName.SIZE;
  public Integer size;
}
