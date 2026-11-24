package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import jakarta.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The <code>definition</code> clause for a table used with {@link
 * io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand} and others.
 *
 * <p>Has the columns and the primary key definition, not the name of the table.
 */
public record TableDefinitionDesc(
    @Schema(
            description =
                "Columns of the table, mapping each column name to its type, for example {\"id\": \"text\", \"title\": \"text\"}",
            type = SchemaType.OBJECT)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty(TableDescConstants.TableDefinitionDesc.COLUMNS)
        ColumnsDescContainer columns,
    @Valid
        @Schema(
            description =
                "Primary key of the table, either the name of one column or an object with partitionBy and partitionSort",
            anyOf = {String.class, PrimaryKeyDesc.class})
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty(TableDescConstants.TableDefinitionDesc.PRIMARY_KEY)
        PrimaryKeyDesc primaryKey) {}
