package io.stargate.sgv2.jsonapi.service.operation.model.impl.builder;

public final class BuiltCondition {

  public ConditionLHS lhs;

  public BuiltConditionPredicate predicate;

  public JsonTerm jsonTerm;

  public BuiltCondition(ConditionLHS lhs, BuiltConditionPredicate predicate, JsonTerm jsonTerm) {
    this.lhs = lhs;
    this.predicate = predicate;
    this.jsonTerm = jsonTerm;
  }

  public static BuiltCondition of(ConditionLHS lhs, BuiltConditionPredicate predicate, JsonTerm jsonTerm) {
    return new BuiltCondition(lhs, predicate, jsonTerm);
  }

  public static BuiltCondition of(String columnName, BuiltConditionPredicate predicate, JsonTerm jsonTerm) {
    return of(ConditionLHS.column(columnName), predicate, jsonTerm);
  }

  @Override
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
    if (jsonTerm != null) {
      builder.append(" ").append(jsonTerm);
    } else {
      builder.append(" null");
    }
    return builder.toString();
  }

}
