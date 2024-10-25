package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The <code>definition</code> clause for a table used with {@link
 * io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand} and others.
 *
 * <p>Has the columns and the primary key definition, not the name of the table.
 */
@JsonPropertyOrder({"name", "definition"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableDefinition(
    @Valid
        @Schema(description = "API table columns definitions", type = SchemaType.OBJECT)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        ColumnsDef columns,
    @Valid
        @Schema(
            description = "Primary key definition for the table",
            anyOf = {String.class, PrimaryKey.class})
        @JsonInclude(JsonInclude.Include.NON_NULL)
        PrimaryKey primaryKey) {}
