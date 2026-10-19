package io.stargate.sgv2.jsonapi.api.model.schema;

import io.quarkus.smallrye.openapi.OpenApiFilter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.microprofile.openapi.OASFactory;
import org.eclipse.microprofile.openapi.OASFilter;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.models.OpenAPI;
import org.jboss.jandex.IndexView;

/** Installs the clause schemas at build time, when Quarkus provides the application class index. */
@OpenApiFilter(stages = OpenApiFilter.RunStage.BUILD)
public class ApiSchemaFilter implements OASFilter {
  private final List<Class<? extends ApiSchemaProvider>> providers;
  private final Set<String> references;

  public ApiSchemaFilter(IndexView index) {
    providers =
        index.getAllKnownImplementations(ApiSchemaProvider.class).stream()
            .<Class<? extends ApiSchemaProvider>>map(
                info -> {
                  try {
                    return Class.forName(
                            info.name().toString(),
                            true,
                            Thread.currentThread().getContextClassLoader())
                        .asSubclass(ApiSchemaProvider.class);
                  } catch (ClassNotFoundException e) {
                    throw new IllegalStateException(
                        "Cannot load API schema provider " + info.name(), e);
                  }
                })
            .toList();
    references =
        providers.stream()
            .map(provider -> "#/components/schemas/" + schemaName(provider))
            .collect(Collectors.toSet());
    if (references.size() != providers.size()) {
      throw new IllegalStateException("API schema providers must have unique schema names");
    }
  }

  @Override
  public org.eclipse.microprofile.openapi.models.media.Schema filterSchema(
      org.eclipse.microprofile.openapi.models.media.Schema schema) {
    if (references.contains(schema.getRef())) {
      // The scanner infers an object from the Java class even when its wire schema allows other
      // types.
      schema.setType(null);
    }
    return schema;
  }

  @Override
  public void filterOpenAPI(OpenAPI openAPI) {
    if (openAPI.getComponents() == null) {
      openAPI.setComponents(OASFactory.createComponents());
    }
    for (var provider : providers) {
      openAPI
          .getComponents()
          .addSchema(schemaName(provider), ApiSchemaProvider.create(provider).schema());
    }
  }

  private static String schemaName(Class<? extends ApiSchemaProvider> provider) {
    var annotation = provider.getAnnotation(Schema.class);
    if (annotation == null || annotation.name().isBlank()) {
      throw new IllegalStateException(
          "API schema provider needs a schema name " + provider.getName());
    }
    return annotation.name();
  }
}
