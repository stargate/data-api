package io.stargate.sgv2.jsonapi.api.model.command.table.definition;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTypeCommand;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * The <code>rename</code> clause for a user-defined type (UDT) used with {@link AlterTypeCommand}.
 *
 * <p>This record represents the renaming of fields in a UDT, where each field can be renamed from
 * an old name to a new name.
 */
public record RenameTypeFieldsDesc(

    /*
     * Map of old field names to new field names.
     * The keys are the current field names, and the values are the new field names to which they
     * should be renamed.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
        @Schema(description = "Map of old field names to new field names")
        Map<String, String> fields) {}
