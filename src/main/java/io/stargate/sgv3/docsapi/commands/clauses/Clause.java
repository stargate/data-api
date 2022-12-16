package io.stargate.sgv3.docsapi.commands.clauses;

/**
 * The command and clauses in the public API aim for a lot of re-use, e.g. filter is used in a lot
 * of places. There may be cases where say filter has some restrictions not in other places, these
 * could be sub classes.
 *
 * <p>Most commands will have their own Options clause, we can have some reuse but the properties
 * depends on the command.
 *
 * <p>Nothing yet, more will come when we start working out the clauses
 */
public abstract class Clause {}
