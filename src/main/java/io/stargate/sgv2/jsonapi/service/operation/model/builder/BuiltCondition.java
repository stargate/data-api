package io.stargate.sgv2.jsonapi.service.operation.model.builder;

// TODO: this is a bit of a mess, it is actually building the query string, prob from the copied old
// code
public final class BuiltCondition {

  public ConditionLHS lhs;

  public BuiltConditionPredicate predicate;

  public BuildConditionTerm rhsTerm;

  public BuiltCondition(
      ConditionLHS lhs, BuiltConditionPredicate predicate, BuildConditionTerm rhsTerm) {
    this.lhs = lhs;
    this.predicate = predicate;
    this.rhsTerm = rhsTerm;
  }

  public static BuiltCondition of(
      ConditionLHS lhs, BuiltConditionPredicate predicate, BuildConditionTerm rhsTerm) {
    return new BuiltCondition(lhs, predicate, rhsTerm);
  }

  public static BuiltCondition of(
      String columnName, BuiltConditionPredicate predicate, BuildConditionTerm rhsTerm) {
    return of(ConditionLHS.column(columnName), predicate, rhsTerm);
  }

  @Override
  // TODO Clarify if this is building the CQL string or just the toString for debugging
  // I think it is jusr debugging becaus the query is build in the QueryBuilder
  public String toString() {
    StringBuilder builder = new StringBuilder();
    // Append the LHS part of the condition
    if (lhs != null) {
      lhs.appendToBuilder(builder);
    } else {
      builder.append("null");
    }
    // Append the predicate part of the condition
    if (predicate != null) {
      builder.append(" ").append(predicate);
    } else {
      builder.append(" null");
    }
    // Append the JSON term part of the condition
    // TODO: What is happening there, the JSON Term does not have a toString ??? I thought this
    // would just be the "?" for params statements
    if (rhsTerm != null) {
      builder.append(" ").append(rhsTerm);
    } else {
      builder.append(" null");
    }
    return builder.toString();
  }
}
