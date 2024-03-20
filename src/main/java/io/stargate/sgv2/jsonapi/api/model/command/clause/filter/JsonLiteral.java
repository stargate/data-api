package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

// Literal value to use as RHS operand in the query
/**
 * @param value Literal value to use as RHS operand in the query
 * @param type Literal value type of the RHS operand in the query
 * @param <T> Data type of the object
 */
public record JsonLiteral<T>(T value, JsonType type) {}
