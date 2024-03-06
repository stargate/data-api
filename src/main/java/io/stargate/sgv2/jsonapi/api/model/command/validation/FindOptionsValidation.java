package io.stargate.sgv2.jsonapi.api.model.command.validation;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

@ApplicationScoped
public class FindOptionsValidation implements ConstraintValidator<CheckFindOption, FindCommand> {

  private final Instance<OperationsConfig> config;

  public FindOptionsValidation(Instance<OperationsConfig> config) {
    this.config = config;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isValid(FindCommand value, ConstraintValidatorContext context) {
    final FindCommand.Options options = value.options();
    if (options == null) return true;

    context.disableDefaultConstraintViolation();
    if (options.skip() != null && (value.sortClause() == null || value.sortClause().isEmpty())) {
      context
          .buildConstraintViolationWithTemplate("skip options should be used with sort clause")
          .addPropertyNode("options.skip")
          .addConstraintViolation();
      return false;
    }

    if (options.skip() != null
        && value.sortClause() != null
        && value.sortClause().hasVsearchClause()) {
      context
          .buildConstraintViolationWithTemplate(
              "skip options should not be used with vector search")
          .addPropertyNode("options.skip")
          .addConstraintViolation();
      return false;
    }

    if (value.sortClause() != null
        && value.sortClause().hasVsearchClause()
        && options.limit() != null
        && options.limit() > config.get().maxVectorSearchLimit()) {
      context
          .buildConstraintViolationWithTemplate(
              "limit options should not be greater than "
                  + config.get().maxVectorSearchLimit()
                  + " for vector search")
          .addPropertyNode("options.limit")
          .addConstraintViolation();
      return false;
    }

    if (options.pageState() != null && value.sortClause() != null) {
      context
          .buildConstraintViolationWithTemplate("pageState is not supported with sort clause")
          .addPropertyNode("options.pageState")
          .addConstraintViolation();
      return false;
    }
    return true;
  }
}
