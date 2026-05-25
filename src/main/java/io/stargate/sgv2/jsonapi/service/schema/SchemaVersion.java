package io.stargate.sgv2.jsonapi.service.schema;

/** A base interface so we can have different schema versions for tables and collections */
public interface SchemaVersion {

  int ordinalValue();
}
