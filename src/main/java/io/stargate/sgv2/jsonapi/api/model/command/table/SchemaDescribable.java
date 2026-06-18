package io.stargate.sgv2.jsonapi.api.model.command.table;

/**
 * interface for classes that can generate a description of schema for a users, as part of the
 * building a response for the user.
 *
 * <p>See {@link io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef} for example.
 *
 * @param <T> Type of schema description that this class can provide.
 */
public interface SchemaDescribable<T extends SchemaDescription> {

  /**
   * Called to get the description of the schema for the user.
   *
   * @param schemaDescSource Where the schema description will be used, implementations can use this
   *     to change the description based on the context.
   * @return The user description of the object.
   */
  T getSchemaDescription(SchemaDescSource schemaDescSource);
}
