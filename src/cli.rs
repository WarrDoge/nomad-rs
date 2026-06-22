// SPDX-License-Identifier: Apache-2.0

//! CLI command parsing contract.
//!
//! Parses an argv slice into a command name and its arguments. The concrete
//! arg-parsing crate lives behind [`parse`]. Behaviour is specified by the
//! tests and is unimplemented.

use crate::error::Result;

/// A parsed CLI invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedCommand {
    /// Command name, e.g. `"job"` or `"node"`.
    pub name: String,
    /// Remaining positional arguments after the command name.
    pub args: Vec<String>,
}

/// Parse an argv slice (excluding the program name) into a [`ParsedCommand`].
///
/// # Errors
///
/// Returns [`crate::error::Error::Config`] if `args` is empty or the command is
/// unknown.
pub fn parse(args: &[String]) -> Result<ParsedCommand> {
    todo!("take the first token as the command name and the rest as args: {args:?}")
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn parses_command_and_args() {
        let argv = vec!["job".to_owned(), "status".to_owned(), "redis".to_owned()];
        let parsed = parse(&argv).unwrap();
        assert_eq!(parsed.name, "job");
        assert_eq!(parsed.args, vec!["status".to_owned(), "redis".to_owned()]);
    }

    #[test]
    fn empty_argv_errors() {
        assert!(parse(&[]).is_err());
    }
}
