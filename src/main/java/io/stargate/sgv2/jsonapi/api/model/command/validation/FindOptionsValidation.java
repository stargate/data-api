package io.stargate.sgv2.jsonapi.api.model.command.validation;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
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
    if (options.skip() != null && value.sortClause() == null) {
      context
          .buildConstraintViolationWithTemplate("skip options should be used with sort clause")
          .addPropertyNode("options.skip")
          .addConstraintViolation();
      return false;
    }

    if (options.skip() != null && SortClauseUtil.resolveVsearch(value.sortClause()) != null) {
      context
          .buildConstraintViolationWithTemplate(
              "skip options should not be used with vector search")
          .addPropertyNode("options.skip")
          .addConstraintViolation();
      return false;
    }

    if (SortClauseUtil.resolveVsearch(value.sortClause()) != null
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

    if (options.pagingState() != null && value.sortClause() != null) {
      context
          .buildConstraintViolationWithTemplate("pagingState is not supported with sort clause")
          .addPropertyNode("options.pagingState")
          .addConstraintViolation();
      return false;
    }
    return true;
  }
}
