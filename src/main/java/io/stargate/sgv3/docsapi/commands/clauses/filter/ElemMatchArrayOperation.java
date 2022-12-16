package io.stargate.sgv3.docsapi.commands.clauses.filter;

import java.util.List;

public class ElemMatchArrayOperation extends ArrayOperation {
  public final ArrayOperationName operator = ArrayOperationName.ELEM_MATCH;
  public List<ComparisonOperation> operations;
}
