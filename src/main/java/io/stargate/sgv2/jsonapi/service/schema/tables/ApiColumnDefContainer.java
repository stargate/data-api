package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;

/**
 * Interface for a container of {@link ApiColumnDef} objects for common usage.
 *
 * <p>Also defines the serializer to use when we need to describe the schema in a response.
 */
@JsonSerialize(using = ApiColumnDefContainerSerializer.class)
public interface ApiColumnDefContainer extends Map<CqlIdentifier, ApiColumnDef> {

  /**
   * Helper method to add a {@link ApiColumnDef} to the container, using the {@link
   * ApiColumnDef#name()} as the key.
   */
  default ApiColumnDef put(ApiColumnDef columnDef) {
    return put(columnDef.name(), columnDef);
  }
}
