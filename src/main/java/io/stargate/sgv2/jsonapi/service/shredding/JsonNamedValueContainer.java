package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;

public interface JsonNamedValueContainer
    extends NamedValueContainer<JsonPath, JsonLiteral<?>, JsonNamedValue> {
}
