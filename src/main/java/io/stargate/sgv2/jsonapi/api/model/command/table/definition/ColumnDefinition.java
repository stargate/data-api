package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.ColumnDefinitionDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnType;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonDeserialize(using = ColumnDefinitionDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example =
        """
                     {"type": "string"}
                      """)
public record ColumnDefinition(
    @NotNull @Schema(description = "Cassandra data type of the column") ColumnType type) {}
