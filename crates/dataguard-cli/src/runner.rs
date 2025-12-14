use std::{
    fs::{self},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use dataguard_core::Validator;
use dataguard_reports::{JsonFormatter, Reporter, StdOutFormatter};
use notify::{
    event::{AccessKind, ModifyKind},
    EventKind, Watcher,
};

use crate::{
    constructor::construct_csv_table, errors::ConfigError, parser::parse_config,
    writer::resolve_file_path, Args, OutputFormat,
};

pub fn run(args: Args) -> Result<bool> {
    let version = env!("CARGO_PKG_VERSION");

    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            let mut formatter = StdOutFormatter::new(version.to_string());
            formatter.on_start();
            execute_validation(&args, &mut formatter)
        }
        OutputFormat::Json => {
            // JSON output format - placeholder for future implementation
            let mut formatter = JsonFormatter::new(version.to_string());
            formatter.on_start();
            let res = execute_validation(&args, &mut formatter)?;
            let output = formatter
                .to_json()
                .with_context(|| "Failed to serialize validation results to JSON")?;
            let output_path = resolve_file_path(&args.path, formatter.get_timestamp_compact())?;
            fs::write(&output_path, output)
                .with_context(|| format!("Failed to write JSON to: {}", output_path.display()))?;
            Ok(res)
        }
    }
}

pub fn watch_run(args: Args) -> Result<bool> {
    let version = env!("CARGO_PKG_VERSION");

    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            let mut reporter = StdOutFormatter::new(version.to_string());
            reporter.on_start();
            run_watch_loop(&args, &mut reporter)?;
        }
        OutputFormat::Json => {
            anyhow::bail!("Watch mode (--watch) is not currently supported with JSON output format. Please use --output stdout for watch mode.");
        }
    }
    Ok(true)
}

fn execute_validation<R: Reporter>(args: &Args, reporter: &mut R) -> Result<bool> {
    let mut validator = Validator::new();
    reporter.on_loading();
    let config = parse_config(args.config.clone())?;
    let n_tables = config.table.len();

    for (i, t) in config.table.iter().enumerate() {
        reporter.on_table_load(i + 1, n_tables, &t.name);
        let csv_table = construct_csv_table(t)
            .with_context(|| format!("Failed to parse table: '{}'", t.name))?;
        validator.add_table(t.name.clone(), csv_table);
    }

    reporter.on_validation_start();
    let res = validator.validate_all()?;

    for r in &res {
        reporter.on_table_result(r);
    }

    let passed = res.iter().filter(|r| r.is_passed()).count();
    let failed = res.len() - passed;
    reporter.on_summary(passed, failed);

    Ok(failed == 0)
}

fn run_watch_loop<R: Reporter>(args: &Args, reporter: &mut R) -> Result<bool> {
    reporter.on_waiting();

    let config = parse_config(args.config.clone())?;
    if config.table.len() > 1 {
        return Err(ConfigError::TooMuchTable {
            n_table: config.table.len(),
        })
        .context("Watch mode only supports single table validation")?;
    }
    let path = PathBuf::from(config.table[0].path.as_str());

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(tx)?;
    watcher.watch(
        Path::new(config.table[0].path.as_str()).parent().unwrap(),
        notify::RecursiveMode::NonRecursive,
    )?;

    let mut modified = false;
    for res in rx {
        match res {
            Ok(event) => match event.kind {
                EventKind::Access(AccessKind::Close(..)) => {
                    if modified {
                        execute_validation(args, reporter)?;
                        reporter.on_waiting();
                        modified = false;
                    }
                }
                EventKind::Modify(ModifyKind::Data(..)) => {
                    if path.file_name().unwrap() == event.paths[0].file_name().unwrap() {
                        modified = true;
                    }
                }
                _ => {}
            },
            Err(e) => println!("error: {}", e),
        }
    }

    Ok(true)
}
