package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that create embedding service configuration.")
@JsonTypeName("createEmbeddingService")
public record CreateEmbeddingServiceCommand(
    @NotNull
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Size(min = 1, max = 48)
        @Schema(description = "Service name to be created")
        String name,
    @Nullable
        @Pattern(
            regexp = "(openai|vertexai|huggingface)",
            message = "supported providers are 'openai', 'vertexai' or 'huggingface'")
        @Schema(
            description = "Embedding service provider name",
            type = SchemaType.STRING,
            implementation = String.class)
        String apiProvider,
    @NotNull @Schema(description = "Api token from the service provider") String apiKey,
    @NotNull @Schema(description = "Base url for the service provider") String baseUrl)
    implements GeneralCommand, NoOptionsCommand {}
