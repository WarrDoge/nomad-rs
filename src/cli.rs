// SPDX-License-Identifier: Apache-2.0

//! CLI command parsing contract.
//!
//! Parses an argv slice into a command name and its arguments. The concrete
//! arg-parsing crate lives behind [`crate::cli::parse`]. Behaviour is specified by the
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
    let Some(first) = args.first() else {
        return Err(crate::error::Error::Config("empty argv: no command provided".to_owned()));
    };

    match first.as_str() {
        "job" | "node" | "server" | "alloc" | "monitor" | "agent" | "status" => {
            Ok(ParsedCommand { name: first.to_owned(), args: args[1..].to_vec() })
        },
        other => Err(crate::error::Error::Config(format!("unknown command '{other}'"))),
    }
}

/// Validate subcommand arguments for common patterns.
///
/// # Errors
///
/// Returns a [`crate::error::Error::Config`] if validation fails.
pub fn validate_subcommand(name: &str, sub: &str) -> Result<()> {
    match name {
        "job" => match sub {
            "run" | "stop" | "status" | "inspect" | "" => Ok(()),
            other => Err(crate::error::Error::Config(format!("unknown job subcommand '{other}'"))),
        },
        "node" => match sub {
            "status" | "drain" | "eligibility" | "" => Ok(()),
            other => Err(crate::error::Error::Config(format!("unknown node subcommand '{other}'"))),
        },
        "server" => match sub {
            "members" | "force-leave" | "join" | "" => Ok(()),
            other => Err(crate::error::Error::Config(format!("unknown server subcommand '{other}'"))),
        },
        "alloc" => match sub {
            "status" | "logs" | "exec" | "" => Ok(()),
            other => Err(crate::error::Error::Config(format!("unknown alloc subcommand '{other}'"))),
        },
        "monitor" | "agent" | "status" => Ok(()),
        _ => Err(crate::error::Error::Config(format!("unknown command '{name}'"))),
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn parses_job_command() {
        let argv = vec!["job".to_owned(), "status".to_owned(), "redis".to_owned()];
        let parsed = parse(&argv).unwrap();
        assert_eq!(parsed.name, "job");
        assert_eq!(parsed.args, vec!["status".to_owned(), "redis".to_owned()]);
    }

    #[test]
    fn parses_node_command() {
        let argv = vec!["node".to_owned(), "status".to_owned()];
        let parsed = parse(&argv).unwrap();
        assert_eq!(parsed.name, "node");
        assert_eq!(parsed.args, vec!["status".to_owned()]);
    }

    #[test]
    fn parses_server_command() {
        let argv = vec!["server".to_owned(), "members".to_owned()];
        let parsed = parse(&argv).unwrap();
        assert_eq!(parsed.name, "server");
        assert_eq!(parsed.args, vec!["members".to_owned()]);
    }

    #[test]
    fn parses_alloc_command() {
        let argv = vec!["alloc".to_owned(), "status".to_owned(), "abc123".to_owned()];
        let parsed = parse(&argv).unwrap();
        assert_eq!(parsed.name, "alloc");
        assert_eq!(parsed.args, vec!["status".to_owned(), "abc123".to_owned()]);
    }

    #[test]
    fn parses_monitor_command() {
        let argv = vec!["monitor".to_owned()];
        let parsed = parse(&argv).unwrap();
        assert_eq!(parsed.name, "monitor");
        assert!(parsed.args.is_empty());
    }

    #[test]
    fn empty_argv_errors() {
        assert!(parse(&[]).is_err());
    }

    #[test]
    fn unknown_command_errors() {
        let argv = vec!["unknown".to_owned()];
        assert!(parse(&argv).is_err());
    }

    #[test]
    fn validate_job_subcommands() {
        assert!(validate_subcommand("job", "run").is_ok());
        assert!(validate_subcommand("job", "stop").is_ok());
        assert!(validate_subcommand("job", "status").is_ok());
        assert!(validate_subcommand("job", "inspect").is_ok());
        assert!(validate_subcommand("job", "unknown").is_err());
    }

    #[test]
    fn validate_node_subcommands() {
        assert!(validate_subcommand("node", "status").is_ok());
        assert!(validate_subcommand("node", "drain").is_ok());
        assert!(validate_subcommand("node", "eligibility").is_ok());
        assert!(validate_subcommand("node", "unknown").is_err());
    }

    #[test]
    fn validate_server_subcommands() {
        assert!(validate_subcommand("server", "members").is_ok());
        assert!(validate_subcommand("server", "force-leave").is_ok());
        assert!(validate_subcommand("server", "join").is_ok());
        assert!(validate_subcommand("server", "unknown").is_err());
    }

    #[test]
    fn validate_alloc_subcommands() {
        assert!(validate_subcommand("alloc", "status").is_ok());
        assert!(validate_subcommand("alloc", "logs").is_ok());
        assert!(validate_subcommand("alloc", "exec").is_ok());
        assert!(validate_subcommand("alloc", "unknown").is_err());
    }

    #[test]
    fn validate_monitor_always_ok() {
        assert!(validate_subcommand("monitor", "").is_ok());
    }

    #[test]
    fn agent_and_status_commands_are_known() {
        let argv = vec!["agent".to_owned()];
        assert!(parse(&argv).is_ok());
        assert_eq!(parse(&argv).unwrap().name, "agent");

        let argv = vec!["status".to_owned()];
        assert!(parse(&argv).is_ok());
        assert_eq!(parse(&argv).unwrap().name, "status");
    }

    #[test]
    fn parses_command_and_args() {
        let argv = vec!["job".to_owned(), "status".to_owned(), "redis".to_owned()];
        let parsed = parse(&argv).unwrap();
        assert_eq!(parsed.name, "job");
        assert_eq!(parsed.args, vec!["status".to_owned(), "redis".to_owned()]);
    }
}
