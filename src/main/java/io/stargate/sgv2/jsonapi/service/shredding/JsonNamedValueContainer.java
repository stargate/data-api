package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;

/**
 * Base interface for {@link JsonNamedValue} containers, so they can be used in a generic way,
 * regardless of order.
 */
public interface JsonNamedValueContainer
    extends NamedValueContainer<JsonPath, JsonLiteral<?>, JsonNamedValue> {}
