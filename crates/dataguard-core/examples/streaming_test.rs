// Simple streaming reader test
// This is a low-level test that directly uses the readers
// For real usage, you would use CsvTable which will eventually have streaming support

use dataguard_core::readers::{read_parallel, read_streaming, FileFormat, ReaderConfig};
use std::time::Instant;

fn main() {
    // Get CSV file path from command line args
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <path-to-csv-file> <column1> [column2] [column3]...",
            args[0]
        );
        eprintln!("Example: {} data.csv id name email", args[0]);
        std::process::exit(1);
    }
    let csv_path = &args[1];
    let column_names: Vec<String> = args[2..].to_vec();

    println!("=== DataGuard Streaming Reader Test ===");
    println!("File: {}", csv_path);
    println!("Columns: {:?}", column_names);
    println!();

    // Get file size
    let file_size = std::fs::metadata(csv_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "File size: {:.2} GB",
        file_size as f64 / 1024.0 / 1024.0 / 1024.0
    );
    println!();

    // Configuration
    let config = ReaderConfig::default();

    // ===== BATCH MODE =====
    println!("--- BATCH MODE (read_parallel) ---");
    let batch_start = Instant::now();

    let batches = match read_parallel(csv_path, column_names.clone(), FileFormat::Csv, &config) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Error reading CSV (batch mode): {}", e);
            std::process::exit(1);
        }
    };

    let batch_total_time = batch_start.elapsed();

    println!("Batches loaded: {}", batches.len());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Total rows: {}", total_rows);
    println!("Total time: {:?}", batch_total_time);
    println!(
        "Throughput: {:.2} MB/s",
        (file_size as f64 / 1024.0 / 1024.0) / batch_total_time.as_secs_f64()
    );
    println!();

    // ===== STREAMING MODE =====
    println!("--- STREAMING MODE (read_streaming) ---");
    let stream_start = Instant::now();

    let receiver = match read_streaming(csv_path, column_names.clone(), FileFormat::Csv, config) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error creating streaming reader: {}", e);
            std::process::exit(1);
        }
    };

    println!("Streaming started, processing mini-batches...");

    // Count rows as they arrive
    let mut stream_total_rows = 0;
    let mut mini_batch_count = 0;
    let mut first_mini_batch_time = None;

    for mini_batch_result in receiver {
        let mini_batch = match mini_batch_result {
            Ok(mb) => mb,
            Err(e) => {
                eprintln!("Error receiving mini-batch: {}", e);
                std::process::exit(1);
            }
        };

        // Record time to first mini-batch
        if first_mini_batch_time.is_none() {
            first_mini_batch_time = Some(stream_start.elapsed());
        }

        mini_batch_count += 1;
        let rows_in_mini_batch: usize = mini_batch.iter().map(|b| b.num_rows()).sum();
        stream_total_rows += rows_in_mini_batch;
    }

    let stream_total_time = stream_start.elapsed();

    println!("Mini-batches received: {}", mini_batch_count);
    println!("Total rows: {}", stream_total_rows);
    println!(
        "Time to first mini-batch: {:?}",
        first_mini_batch_time.unwrap()
    );
    println!("Total time: {:?}", stream_total_time);
    println!(
        "Throughput: {:.2} MB/s",
        (file_size as f64 / 1024.0 / 1024.0) / stream_total_time.as_secs_f64()
    );
    println!();

    // ===== COMPARISON =====
    println!("--- COMPARISON ---");
    println!("Batch mode total:     {:?}", batch_total_time);
    println!("Streaming mode total: {:?}", stream_total_time);

    if stream_total_time.as_secs_f64() > 0.0 {
        let speedup = batch_total_time.as_secs_f64() / stream_total_time.as_secs_f64();
        println!(
            "Speedup: {:.2}x {}",
            speedup,
            if speedup > 1.0 {
                "(streaming faster)"
            } else {
                "(batch faster)"
            }
        );
    }

    println!();
    println!("Rows match: {}", total_rows == stream_total_rows);

    if total_rows != stream_total_rows {
        eprintln!("WARNING: Row count mismatch!");
        eprintln!("  Batch: {}", total_rows);
        eprintln!("  Streaming: {}", stream_total_rows);
    }
}
