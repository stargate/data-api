package io.stargate.sgv2.jsonapi.api.model.command.validation;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class FindOptionsValidation implements ConstraintValidator<CheckFindOption, FindCommand> {

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
