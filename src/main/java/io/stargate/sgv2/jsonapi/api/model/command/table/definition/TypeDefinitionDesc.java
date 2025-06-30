package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.TypeFieldsContainerDeserializer;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTypeCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTypeCommand;
import jakarta.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The <code>definition</code> clause for a user-defined type(UDT) used with {@link
 * CreateTypeCommand} and {@link AlterTypeCommand}.
 */
public record TypeDefinitionDesc(
    @Valid
        @Schema(description = "fields definitions of a user-defined type", type = SchemaType.OBJECT)
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonDeserialize(using = TypeFieldsContainerDeserializer.class)
        // UDT field and Table column are both represented as ColumnDesc.
        ColumnsDescContainer fields) {}
