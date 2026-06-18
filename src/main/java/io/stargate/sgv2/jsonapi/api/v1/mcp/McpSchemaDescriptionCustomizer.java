package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import io.quarkiverse.mcp.server.runtime.SchemaGeneratorConfigCustomizer;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Enables MicroProfile {@link Schema#description()} resolution for the victools JSON schema
 * generator used by Quarkus MCP Server. Without this customizer, nested record fields annotated
 * with {@code @Schema(description = "...")} will not have their descriptions included in the MCP
 * tool schema output.
 *
 * <p>The Quarkus MCP Server's {@code DefaultSchemaGenerator} uses {@code OptionPreset.PLAIN_JSON},
 * which does not read any annotations by default. This customizer bridges that gap by adding a
 * field-level description resolver for MicroProfile OpenAPI's {@code @Schema} annotation.
 */
@Singleton
public class McpSchemaDescriptionCustomizer implements SchemaGeneratorConfigCustomizer {

  @Override
  public void customize(SchemaGeneratorConfigBuilder builder) {
    builder
        .forFields()
        .withDescriptionResolver(
            field -> {
              Schema schemaAnn = field.getAnnotationConsideringFieldAndGetter(Schema.class);
              return schemaAnn != null && !schemaAnn.description().isEmpty()
                  ? schemaAnn.description()
                  : null;
            });
  }
}
