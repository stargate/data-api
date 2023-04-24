package io.stargate.sgv2.jsonapi.api.model.command.validation;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/** Validator for the {@link MaxInsertManyDocuments} annotation. */
@ApplicationScoped
public class MaxInsertManyDocumentsValidation
    implements ConstraintValidator<MaxInsertManyDocuments, List<JsonNode>> {

  private final Instance<OperationsConfig> config;

  public MaxInsertManyDocumentsValidation(Instance<OperationsConfig> config) {
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isValid(List<JsonNode> value, ConstraintValidatorContext context) {
    final int max = config.get().maxDocumentInsertCount();
    if (null != value && value.size() > max) {
      context
          .buildConstraintViolationWithTemplate(
              String.format(
                  "%s (%d vs %d)",
                  context.getDefaultConstraintMessageTemplate(), value.size(), max))
          .addConstraintViolation()
          .disableDefaultConstraintViolation();
      return false;
    }
    return true;
  }
}
