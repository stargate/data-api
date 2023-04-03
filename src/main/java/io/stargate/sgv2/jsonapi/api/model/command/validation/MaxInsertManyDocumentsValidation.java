package io.stargate.sgv2.jsonapi.api.model.command.validation;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/** Validator for the {@link MaxInsertManyDocuments} annotation. */
@ApplicationScoped
public class MaxInsertManyDocumentsValidation
    implements ConstraintValidator<MaxInsertManyDocuments, List<JsonNode>> {

  private final Instance<DocumentLimitsConfig> config;

  public MaxInsertManyDocumentsValidation(Instance<DocumentLimitsConfig> config) {
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isValid(List<JsonNode> value, ConstraintValidatorContext context) {
    DocumentLimitsConfig limitsConfig = config.get();
    return null != value && value.size() <= limitsConfig.maxInsertManyDocuments();
  }
}
