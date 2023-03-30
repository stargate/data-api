package io.stargate.sgv2.jsonapi.api.model.command.validation;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Validator for the {@link MaxInsertManyDocuments} annotation. */
@ApplicationScoped
public class MaxInsertManyDocumentsValidation
    implements ConstraintValidator<MaxInsertManyDocuments, List<JsonNode>> {

  private final int limit;

  // due to https://github.com/quarkusio/quarkus/issues/32265
  // I can only inject prop, not the whole config class
  public MaxInsertManyDocumentsValidation(
      @ConfigProperty(name = "stargate.jsonapi.doc-limits.max-insert-many-documents") int limit) {
    this.limit = limit;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isValid(List<JsonNode> value, ConstraintValidatorContext context) {
    return null != value && value.size() <= limit;
  }
}
