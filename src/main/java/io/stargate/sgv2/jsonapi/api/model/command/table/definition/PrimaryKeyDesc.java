package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.PrimaryKeyDescDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.serializer.OrderingKeyDescSerializer;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiClusteringOrder;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * <b>NOTE:</b> use the `from()` functions to construct when you have the CQL Identifiers so there
 * is a guarantee that the keys are properly formatted.
 *
 * @param keys
 * @param orderingKeys
 */
@JsonDeserialize(using = PrimaryKeyDescDeserializer.class)
// https://github.com/stargate/data-api/pull/1360
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    description = "Represents the table primary key")
public record PrimaryKeyDesc(
    @NotNull
        @Schema(description = "Columns that make the partition keys", type = SchemaType.ARRAY)
        @JsonProperty(TableDescConstants.PrimaryKey.PARTITION_BY)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String[] keys,
    @Nullable
        @Schema(description = "Columns that make the ordering keys", type = SchemaType.ARRAY)
        @JsonProperty(TableDescConstants.PrimaryKey.PARTITION_SORT)
        @JsonSerialize(using = OrderingKeyDescSerializer.class)
        OrderingKeyDesc[] orderingKeys) {

  public static PrimaryKeyDesc from(
      List<CqlIdentifier> partitionKeys, List<OrderingKeyDesc> orderingKeyDescs) {
    return new PrimaryKeyDesc(
        partitionKeys.stream()
            .map(CqlIdentifierUtil::cqlIdentifierToJsonKey)
            .toArray(String[]::new),
        orderingKeyDescs.toArray(new OrderingKeyDesc[0]));
  }

  public record OrderingKeyDesc(String column, Order order) {

    public enum Order {
      @JsonProperty("1")
      ASC(1),
      @JsonProperty("-1")
      DESC(-1);

      public final int ordinal;

      Order(int ordinal) {
        this.ordinal = ordinal;
      }

      public static Order from(ApiClusteringOrder order) {
        return switch (order) {
          case ASC -> ASC;
          case DESC -> DESC;
        };
      }

      public static Optional<Order> fromUserDesc(int order) {
        return switch (order) {
          case 1 -> Optional.of(ASC);
          case -1 -> Optional.of(DESC);
          default -> Optional.empty();
        };
      }
    }

    public static OrderingKeyDesc from(CqlIdentifier column, ApiClusteringOrder clusteringOrder) {
      return new OrderingKeyDesc(cqlIdentifierToJsonKey(column), Order.from(clusteringOrder));
    }
  }
}
