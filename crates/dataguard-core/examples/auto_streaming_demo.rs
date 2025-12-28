//! Example demonstrating automatic streaming mode selection
//!
//! This example shows how CsvTable automatically chooses between:
//! - **Batch mode**: For files < 500MB (default threshold)
//! - **Streaming mode**: For files >= 500MB (default threshold)
//!
//! Run with:
//! ```bash
//! cargo run --example auto_streaming_demo --release /path/to/file.csv
//! ```

use dataguard_core::{CsvTable, NumericColumnBuilder, StringColumnBuilder, Table};
use std::env;
use std::fs::File;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <csv_file_path>", args[0]);
        eprintln!("\nExample:");
        eprintln!("  cargo run --example auto_streaming_demo --release benchmark/ecommerce_data_medium.csv");
        std::process::exit(1);
    }

    let file_path = &args[1];

    // Check file size
    let file = File::open(file_path)?;
    let file_size = file.metadata()?.len();
    drop(file);

    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);
    let threshold_mb = 500.0;

    println!("=== DataGuard Auto-Streaming Demo ===");
    println!("File: {}", file_path);
    println!("File size: {:.2} MB", file_size_mb);
    println!("Streaming threshold: {:.0} MB", threshold_mb);
    println!();

    if file_size_mb >= threshold_mb {
        println!("✓ File is >= threshold → Using STREAMING mode");
        println!("  - Lower latency to first result");
        println!("  - Bounded memory usage");
        println!("  - Sequential chunk processing");
    } else {
        println!("✓ File is < threshold → Using BATCH mode");
        println!("  - Higher total throughput");
        println!("  - Parallel chunk processing");
        println!("  - All data loaded into memory");
    }
    println!();

    // Configure validation rules
    let mut customer_id_col = StringColumnBuilder::new("customer_id".to_string());
    customer_id_col.is_not_null(0.0);

    let mut customer_name_col = StringColumnBuilder::new("customer_name".to_string());
    customer_name_col.with_min_length(2, 0.0);

    let mut price_col = NumericColumnBuilder::<f64>::new("product_price".to_string());
    price_col.is_non_negative(0.0);

    // Create CsvTable and prepare validation
    let mut csv_table = CsvTable::new(file_path.to_string(), "ecommerce".to_string())?;
    csv_table.prepare(
        vec![
            Box::new(customer_id_col),
            Box::new(customer_name_col),
            Box::new(price_col),
        ],
        vec![],
    )?;

    println!("Running validation...");
    let start = Instant::now();

    // CsvTable::validate() automatically chooses batch vs streaming
    let result = csv_table.validate()?;

    let elapsed = start.elapsed();

    println!();
    println!("=== Validation Results ===");
    println!("Table: {}", result.table_name);
    println!("Total rows: {}", result.total_rows);
    println!("Time elapsed: {:.3}s", elapsed.as_secs_f64());
    println!(
        "Throughput: {:.2} MB/s",
        file_size_mb / elapsed.as_secs_f64()
    );
    println!();

    let (passed, total) = result.is_passed();
    println!("Rules passed: {}/{}", passed, total);

    if passed == total {
        println!("✓ All validation rules passed!");
    } else {
        println!("✗ Some validation rules failed");
        println!();
        println!("Column Results:");
        for (column, rules) in result.get_column_results() {
            println!("  {}:", column);
            for rule in rules {
                let status = if rule.pass { "✓" } else { "✗" };
                println!(
                    "    {} {} - errors: {} ({:.2}% vs {:.2}% threshold)",
                    status, rule.rule_name, rule.error_count, rule.error_percentage, rule.threshold
                );
            }
        }
    }

    Ok(())
}
