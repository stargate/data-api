package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.PrimaryKeyDeserializer;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonDeserialize(using = PrimaryKeyDeserializer.class)
// TODO, hide table feature detail before it goes public,
// https://github.com/stargate/data-api/pull/1360
// @Schema(
//    type = SchemaType.OBJECT,
//    implementation = Object.class,
//    description = "Represents the table primary key")
public record PrimaryKey(
    @NotNull @Schema(description = "Columns that make the partition keys", type = SchemaType.ARRAY)
        String[] keys,
    @Nullable @Schema(description = "Columns that make the ordering keys", type = SchemaType.ARRAY)
        OrderingKey[] orderingKeys) {

  public record OrderingKey(String column, Order order) {
    public enum Order {
      @JsonProperty("1")
      ASC,
      @JsonProperty("-1")
      DESC;
    }
  }
}
