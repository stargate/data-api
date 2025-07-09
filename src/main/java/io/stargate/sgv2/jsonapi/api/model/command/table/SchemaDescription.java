package io.stargate.sgv2.jsonapi.api.model.command.table;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;

/**
 * Marker interface for a class that is used to describe the schema with users.
 *
 * <p>See {@link SchemaDescribable}
 *
 * <p>Used for when we are parsing the incoming JSON from a user request, or when building a
 * response for the user.
 *
 * <p>Note: would be nice to expode the {@link SchemaDescSource} as a field, but we have some
 * situations where the description is the same regardless of the source, and we want to cache those
 * descriptions, ( see {@link PrimitiveColumnDesc} so excluding for now until we have a need for it.
 */
public interface SchemaDescription {}
