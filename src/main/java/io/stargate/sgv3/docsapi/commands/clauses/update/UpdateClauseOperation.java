package io.stargate.sgv3.docsapi.commands.clauses.update;

/** Base for document update operators, such as $set or $min */
public abstract class UpdateClauseOperation {

  // the name used in the query, e.g. $set
  public abstract String operatorName();
}
