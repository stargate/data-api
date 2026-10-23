package io.stargate.sgv2.jsonapi.config.constants;

/** Constants for reusing the OpenAPI objects. */
public interface OpenApiConstants {

  /** Name for the Open API default security scheme. */
  interface SecuritySchemes {

    String TOKEN = "Token";
  }

  /** Locations of the reusable schemas in the OpenAPI document. */
  interface Components {

    /** JSON Pointer to the reusable schemas, for reading them from the document. */
    String SCHEMAS_POINTER = "/components/schemas/";

    /** Start of a {@code $ref} to a reusable schema. Add the schema name to the end. */
    String SCHEMA_REF_PREFIX = "#" + SCHEMAS_POINTER;
  }
}
