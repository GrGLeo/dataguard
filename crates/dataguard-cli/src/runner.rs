use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use dataguard_core::{rules::Monotonicity, Validator};
use dataguard_reports::StdOutFormatter;
use notify::{event::{AccessKind, ModifyKind}, Event, EventKind, Watcher};

use crate::{
    constructor::construct_csv_table, errors::ConfigError, parser::parse_config, Args, OutputFormat,
};

pub fn run(args: Args) -> Result<bool> {
    let mut validator = Validator::new();

    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            let version = env!("CARGO_PKG_VERSION");
            let formatter = StdOutFormatter::new(version.to_string());
            formatter.print_loading_start();
            let config = parse_config(args.config)?;
            let n_table = config.table.len();
            for (i, t) in config.table.iter().enumerate() {
                formatter.print_loading_progress(i + 1, n_table, &t.name);
                let csv_table = construct_csv_table(t)
                    .with_context(|| format!("Failed to parse table: '{}'", t.name))?;
                validator.add_table(t.name.clone(), csv_table);
            }
            formatter.print_validation_start();
            let res = validator.validate_all()?;

            for r in &res {
                formatter.print_table_result(r);
            }
            let passed = res.iter().filter(|r| r.is_passed()).count();
            let failed = res.len() - passed;
            formatter.print_summary(passed, failed);

            Ok(failed == 0)
        }
        OutputFormat::Json => {
            // JSON output format - placeholder for future implementation
            let config = parse_config(args.config)?;
            for t in config.table {
                println!("Parsing: {}", t.name);
                let csv_table = construct_csv_table(&t)
                    .with_context(|| format!("Failed to parse table: '{}'", t.name))?;
                validator.add_table(t.name, csv_table);
            }
            Ok(true)
        }
    }
}

pub fn watch_run(args: Args) -> Result<bool> {
    let mut validator = Validator::new();

    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            let version = env!("CARGO_PKG_VERSION");
            let formatter = StdOutFormatter::new(version.to_string());
            formatter.print_loading_start();
            let config = parse_config(args.config)?;
            let n_table = config.table.len();
            //if n_table > 1 {
            //    return Err(ConfigError::TooMuchTable { n_table  });
            //}
            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher = notify::recommended_watcher(tx)?;
            let path = PathBuf::from(config.table[0].path.as_str());
            watcher.watch(
                Path::new(config.table[0].path.as_str()).parent().unwrap(),
                notify::RecursiveMode::NonRecursive,
            )?;
            let (mut opened, mut modified) = (false, false);
            for res in rx {
                match res {
                    Ok(event) => {
                        println!("event: {:?}", event);
                        let kind = event.kind;
                        match kind {
                            EventKind::Access(ak) => {
                                match ak {
                                    AccessKind::Open(_) => {
                                        println!("{:?} | {:?}", path.file_name().unwrap(), event.paths);
                                        // println!("{:?} | {:?}", path, event.paths[0]);
                                        if path == event.paths[0] {
                                            println!("open file");
                                            opened = true;
                                        }
                                    }
                                    AccessKind::Close(_) => {
                                        if modified {
                                            println!("ready to validate")
                                        }
                                    }
                                    _ => {},
                                }
                            },
                            EventKind::Modify(mk) => {
                                match mk {
                                    ModifyKind::Data(_) => {
                                        println!("modif file");
                                        if path.file_name().unwrap() == event.paths[0].file_name().unwrap() {
                                            modified = true;
                                        }
                                    },
                                    _ => {},
                                }
                            },
                            _ => {},
                        }
                    }
                    Err(e) => println!("error: {}", e),
                }
            }
        }
        OutputFormat::Json => {}
    }
    Ok(true)
}
