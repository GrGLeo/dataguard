//! Unified File Reader
//!
//! Single module for reading CSV and Parquet files in sequential, parallel, or streaming modes.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │              Unified Reader API                      │
//! ├──────────────┬──────────────┬───────────────────────┤
//! │ Sequential   │  Parallel    │    Streaming          │
//! │ (1 thread)   │ (N threads)  │  (bounded memory)     │
//! └──────┬───────┴──────┬───────┴──────┬────────────────┘
//!        │              │              │
//!   ┌────▼────┐    ┌────▼────┐    ┌───▼────┐
//!   │   CSV   │    │   CSV   │    │  CSV   │
//!   │ Parquet │    │ Parquet │    │Parquet │
//!   └─────────┘    └─────────┘    └────────┘
//! ```
//!
//! ## Usage
//!
//! ```no_run
//! use dataguard_core::readers::{read_parallel, FileFormat, ReaderConfig};
//!
//! let config = ReaderConfig::default();
//! let batches = read_parallel("data.csv", vec!["col1".to_string()], FileFormat::Csv, &config)?;
//! # Ok::<(), std::io::Error>(())
//! ```

use arrow::csv::ReaderBuilder as CsvReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam::channel::{bounded, Receiver, Sender};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use rayon::prelude::*;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::thread;

use crate::readers::config::{calculate_chunk_size, ReaderConfig};

/// Number of batches to collect before sending as a mini-batch (streaming)
const MINI_BATCH_SIZE: usize = 4;

/// Channel capacity for streaming (2× mini-batch size for buffering)
const CHANNEL_CAPACITY: usize = 8;

/// File format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Parquet,
}

impl FileFormat {
    /// Detect format from file extension
    pub fn from_path(path: &str) -> Option<Self> {
        let path_lower = path.to_lowercase();
        if path_lower.ends_with(".csv") {
            Some(FileFormat::Csv)
        } else if path_lower.ends_with(".parquet") {
            Some(FileFormat::Parquet)
        } else {
            None
        }
    }
}

// ============================================================================
// CSV Helper Functions
// ============================================================================

/// Generate UTF-8 schema from CSV file header
fn csv_generate_schema(path: &str) -> Result<Schema, io::Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    if let Some(first) = lines.next() {
        let header = first?;
        let all_columns: Vec<&str> = header.split(',').collect();
        let fields: Vec<Field> = all_columns
            .iter()
            .map(|c| Field::new(c.trim(), DataType::Utf8, true))
            .collect();
        Ok(Schema::new(fields))
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "CSV file is empty",
        ))
    }
}

/// Calculate column projection indices
fn csv_calculate_projection(schema: &Schema, requested_cols: &[&str]) -> Vec<usize> {
    requested_cols
        .iter()
        .filter_map(|col_name| schema.column_with_name(col_name).map(|(idx, _)| idx))
        .collect()
}

/// Create chunks for parallel CSV reading
fn csv_create_chunks(
    path: &str,
    header_len: u64,
    file_size: u64,
    chunk_size: u64,
) -> Result<Vec<(u64, u64)>, io::Error> {
    let mut file = File::open(path)?;
    let mut chunks = Vec::new();
    let mut current = header_len;

    while current < file_size {
        let target_end = (current + chunk_size).min(file_size);

        let actual_end = if target_end >= file_size {
            file_size
        } else {
            csv_find_next_newline(&mut file, target_end)?
        };

        chunks.push((current, actual_end));
        current = actual_end;
    }

    Ok(chunks)
}

/// Find next newline in CSV file
fn csv_find_next_newline(file: &mut File, pos: u64) -> Result<u64, io::Error> {
    file.seek(SeekFrom::Start(pos))?;
    let mut reader = BufReader::new(file.try_clone()?);
    let mut offset = 0u64;
    let mut buffer = Vec::new();

    reader.read_until(b'\n', &mut buffer)?;
    offset += buffer.len() as u64;

    Ok(pos + offset)
}

/// Parse a single CSV chunk
fn csv_parse_chunk(
    path: &str,
    schema: &Arc<Schema>,
    projection: &[usize],
    batch_size: usize,
    header: &str,
    start: u64,
    end: u64,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;

    let chunk_size = (end - start) as usize;
    let mut buffer = Vec::with_capacity(chunk_size + header.len());

    buffer.extend_from_slice(header.as_bytes());

    let mut limited = file.take(end - start);
    limited.read_to_end(&mut buffer)?;

    let cursor = io::Cursor::new(buffer);
    let reader = CsvReaderBuilder::new(schema.clone())
        .with_header(true)
        .with_projection(projection.to_vec())
        .with_batch_size(batch_size)
        .build(cursor)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }

    Ok(batches)
}

/// Read CSV sequentially
fn csv_read_sequential(
    path: &str,
    cols: &[String],
    config: &ReaderConfig,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let cols: Vec<&str> = cols.iter().map(|v| v.as_str()).collect();
    let file = File::open(path)?;
    let schema = Arc::new(csv_generate_schema(path)?);
    let projection = csv_calculate_projection(&schema, cols.as_slice());

    let reader = CsvReaderBuilder::new(schema)
        .with_header(true)
        .with_projection(projection)
        .with_batch_size(config.batch_size as usize)
        .build(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }

    Ok(batches)
}

/// Read CSV in parallel
fn csv_read_parallel(
    path: &str,
    cols: &[String],
    config: &ReaderConfig,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let cols: Vec<&str> = cols.iter().map(|v| v.as_str()).collect();
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let schema = Arc::new(csv_generate_schema(path)?);
    let projection = csv_calculate_projection(&schema, cols.as_slice());

    let mut header_reader = BufReader::new(File::open(path)?);
    let mut header = String::new();
    header_reader.read_line(&mut header)?;
    let header_len = header.len() as u64;

    let num_threads = rayon::current_num_threads();
    let chunk_size = calculate_chunk_size(file_size, header_len, num_threads, config);
    let chunks = csv_create_chunks(path, header_len, file_size, chunk_size)?;

    let batches: Result<Vec<_>, _> = chunks
        .into_par_iter()
        .map(|(start, end)| {
            csv_parse_chunk(
                path,
                &schema,
                &projection,
                config.batch_size as usize,
                &header,
                start,
                end,
            )
        })
        .collect();

    Ok(batches?.into_iter().flatten().collect())
}

/// Read CSV in streaming mode
fn csv_read_streaming(
    path: &str,
    cols: &[String],
    config: &ReaderConfig,
    sender: &Sender<Result<Vec<Arc<RecordBatch>>, io::Error>>,
) -> Result<(), io::Error> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let schema = Arc::new(csv_generate_schema(path)?);

    let cols_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
    let projection = csv_calculate_projection(&schema, &cols_refs);

    let mut header_reader = BufReader::new(File::open(path)?);
    let mut header = String::new();
    header_reader.read_line(&mut header)?;
    let header_len = header.len() as u64;

    let num_threads = rayon::current_num_threads();
    let chunk_size = calculate_chunk_size(file_size, header_len, num_threads, config);

    let chunks = csv_create_chunks(path, header_len, file_size, chunk_size)?;
    let mut mini_batch: Vec<Arc<RecordBatch>> = Vec::with_capacity(MINI_BATCH_SIZE);

    for (start, end) in chunks {
        let chunk_batches = csv_parse_chunk(
            path,
            &schema,
            &projection,
            config.batch_size as usize,
            &header,
            start,
            end,
        )?;

        for batch in chunk_batches {
            mini_batch.push(batch);

            if mini_batch.len() >= MINI_BATCH_SIZE {
                // Take ownership to avoid clone
                let batch_to_send =
                    std::mem::replace(&mut mini_batch, Vec::with_capacity(MINI_BATCH_SIZE));
                if sender.send(Ok(batch_to_send)).is_err() {
                    // Consumer dropped receiver - stop producing
                    return Ok(());
                }
            }
        }
    }

    // Send remaining batches
    if !mini_batch.is_empty() {
        sender
            .send(Ok(mini_batch))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Consumer closed channel"))?;
    }

    Ok(())
}

// ============================================================================
// Parquet Helper Functions
// ============================================================================

/// Create projection mask for Parquet
fn parquet_create_projection_mask(schema: &SchemaDescriptor, cols: &[&str]) -> ProjectionMask {
    let valid_cols: Vec<&str> = cols.iter().filter(|c| !c.is_empty()).copied().collect();

    if valid_cols.is_empty() {
        return ProjectionMask::roots(schema, vec![]);
    }
    ProjectionMask::columns(schema, valid_cols.iter().copied())
}

/// Read a single Parquet row group
fn parquet_read_row_group(
    path: &str,
    row_group_idx: usize,
    projection: ProjectionMask,
    batch_size: usize,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let reader = builder
        .with_projection(projection)
        .with_row_groups(vec![row_group_idx])
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }
    Ok(batches)
}

/// Read Parquet sequentially
fn parquet_read_sequential(
    path: &str,
    cols: &[String],
    config: &ReaderConfig,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let cols: Vec<&str> = cols.iter().map(|v| v.as_str()).collect();
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let projection = parquet_create_projection_mask(builder.parquet_schema(), cols.as_slice());

    let reader = builder
        .with_projection(projection)
        .with_batch_size(config.batch_size as usize)
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }
    Ok(batches)
}

/// Read Parquet in parallel
fn parquet_read_parallel(
    path: &str,
    cols: &[String],
    config: &ReaderConfig,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let cols: Vec<&str> = cols.iter().map(|v| v.as_str()).collect();

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let num_row_groups = builder.metadata().num_row_groups();
    let projection = parquet_create_projection_mask(builder.parquet_schema(), cols.as_slice());
    let row_group_indices: Vec<usize> = (0..num_row_groups).collect();

    let batches: Result<Vec<_>, _> = row_group_indices
        .into_par_iter()
        .map(|rg_idx| {
            parquet_read_row_group(path, rg_idx, projection.clone(), config.batch_size as usize)
        })
        .collect();

    Ok(batches?.into_iter().flatten().collect())
}

/// Read Parquet in streaming mode
fn parquet_read_streaming(
    path: &str,
    cols: &[String],
    config: &ReaderConfig,
    sender: &Sender<Result<Vec<Arc<RecordBatch>>, io::Error>>,
) -> Result<(), io::Error> {
    let cols: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let num_row_groups = builder.metadata().num_row_groups();
    let projection = parquet_create_projection_mask(builder.parquet_schema(), &cols);

    let mut mini_batch: Vec<Arc<RecordBatch>> = Vec::with_capacity(MINI_BATCH_SIZE);

    for rg_idx in 0..num_row_groups {
        let row_group_batches =
            parquet_read_row_group(path, rg_idx, projection.clone(), config.batch_size as usize)?;

        for batch in row_group_batches {
            mini_batch.push(batch);

            if mini_batch.len() >= MINI_BATCH_SIZE {
                // Take ownership to avoid clone
                let batch_to_send =
                    std::mem::replace(&mut mini_batch, Vec::with_capacity(MINI_BATCH_SIZE));
                if sender.send(Ok(batch_to_send)).is_err() {
                    // Consumer dropped receiver - stop producing
                    return Ok(());
                }
            }
        }
    }

    // Send remaining batches
    if !mini_batch.is_empty() {
        sender
            .send(Ok(mini_batch))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Consumer closed channel"))?;
    }

    Ok(())
}

// ============================================================================
// Public API
// ============================================================================

/// Read file sequentially in a single thread
///
/// # Arguments
///
/// * `path` - Path to the file
/// * `cols` - Column names to read
/// * `format` - File format (CSV or Parquet)
///
/// # Returns
///
/// Vector of RecordBatches
pub fn read_sequential(
    path: &str,
    cols: Vec<String>,
    format: FileFormat,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let config = ReaderConfig::default();
    match format {
        FileFormat::Csv => csv_read_sequential(path, &cols, &config),
        FileFormat::Parquet => parquet_read_sequential(path, &cols, &config),
    }
}

/// Read file in parallel using multiple threads
///
/// # Arguments
///
/// * `path` - Path to the file
/// * `cols` - Column names to read
/// * `format` - File format (CSV or Parquet)
/// * `config` - Reader configuration
///
/// # Returns
///
/// Vector of RecordBatches
pub fn read_parallel(
    path: &str,
    cols: Vec<String>,
    format: FileFormat,
    config: &ReaderConfig,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    match format {
        FileFormat::Csv => csv_read_parallel(path, &cols, config),
        FileFormat::Parquet => parquet_read_parallel(path, &cols, config),
    }
}

/// Read file in streaming mode
///
/// # Arguments
///
/// * `path` - Path to the file
/// * `cols` - Column names to read
/// * `format` - File format (CSV or Parquet)
/// * `config` - Reader configuration
///
/// # Returns
///
/// Receiver yielding mini-batches as they become available
pub fn read_streaming(
    path: &str,
    cols: Vec<String>,
    format: FileFormat,
    config: ReaderConfig,
) -> Result<Receiver<Result<Vec<Arc<RecordBatch>>, io::Error>>, io::Error> {
    let (sender, receiver) = bounded(CHANNEL_CAPACITY);
    let path = path.to_string();

    thread::spawn(move || {
        let result = match format {
            FileFormat::Csv => csv_read_streaming(&path, &cols, &config, &sender),
            FileFormat::Parquet => parquet_read_streaming(&path, &cols, &config, &sender),
        };

        if let Err(e) = result {
            let _ = sender.send(Err(e));
        }
    });

    Ok(receiver)
}

/// Auto-detect format and read sequentially
pub fn read_sequential_auto(
    path: &str,
    cols: Vec<String>,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let format = FileFormat::from_path(path).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Could not detect file format from extension",
        )
    })?;

    read_sequential(path, cols, format)
}

/// Auto-detect format and read in parallel
pub fn read_parallel_auto(
    path: &str,
    cols: Vec<String>,
    config: &ReaderConfig,
) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let format = FileFormat::from_path(path).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Could not detect file format from extension",
        )
    })?;

    read_parallel(path, cols, format, config)
}

/// Auto-detect format and read in streaming mode
pub fn read_streaming_auto(
    path: &str,
    cols: Vec<String>,
    config: ReaderConfig,
) -> Result<Receiver<Result<Vec<Arc<RecordBatch>>, io::Error>>, io::Error> {
    let format = FileFormat::from_path(path).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Could not detect file format from extension",
        )
    })?;

    read_streaming(path, cols, format, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    // ========================================================================
    // Format Detection Tests
    // ========================================================================

    #[test]
    fn test_file_format_detection() {
        assert_eq!(FileFormat::from_path("data.csv"), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_path("data.CSV"), Some(FileFormat::Csv));
        assert_eq!(
            FileFormat::from_path("data.parquet"),
            Some(FileFormat::Parquet)
        );
        assert_eq!(
            FileFormat::from_path("data.PARQUET"),
            Some(FileFormat::Parquet)
        );
        assert_eq!(FileFormat::from_path("data.txt"), None);
        assert_eq!(FileFormat::from_path("data"), None);
    }

    // ========================================================================
    // CSV Tests
    // ========================================================================

    #[test]
    fn test_csv_generate_schema_valid() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age,city").unwrap();
        writeln!(file, "Alice,30,New York").unwrap();

        let schema = csv_generate_schema(file.path().to_str().unwrap()).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");
    }

    #[test]
    fn test_csv_generate_schema_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let result = csv_generate_schema(file.path().to_str().unwrap());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_csv_sequential_valid() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age").unwrap();
        writeln!(file, "Alice,30").unwrap();
        writeln!(file, "Bob,25").unwrap();

        let batches = read_sequential(
            file.path().to_str().unwrap(),
            vec!["name".to_string(), "age".to_string()],
            FileFormat::Csv,
        )
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_csv_sequential_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let result = read_sequential(
            file.path().to_str().unwrap(),
            vec!["name".to_string()],
            FileFormat::Csv,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_csv_sequential_invalid_path() {
        let result = read_sequential("nonexistent.csv", vec!["name".to_string()], FileFormat::Csv);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    // ========================================================================
    // Parquet Tests
    // ========================================================================

    fn get_parquet_test_file() -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests");
        path.push("fixtures");
        path.push("test_ecommerce_data.parquet");
        path
    }

    #[test]
    fn test_parquet_sequential_all_columns() {
        let test_file = get_parquet_test_file();
        let batches = read_sequential(
            test_file.to_str().unwrap(),
            vec![
                String::from("id"),
                String::from("name"),
                String::from("value"),
            ],
            FileFormat::Parquet,
        )
        .unwrap();

        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 512_000);
        assert_eq!(batches[0].num_columns(), 3);
    }

    #[test]
    fn test_parquet_parallel_all_columns() {
        let test_file = get_parquet_test_file();
        let config = ReaderConfig::default();

        let batches = read_parallel(
            test_file.to_str().unwrap(),
            vec![
                String::from("id"),
                String::from("name"),
                String::from("value"),
            ],
            FileFormat::Parquet,
            &config,
        )
        .unwrap();

        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 512_000);
    }

    #[test]
    fn test_parquet_parallel_vs_sequential() {
        let test_file = get_parquet_test_file();
        let test_file_str = test_file.to_str().unwrap();
        let config = ReaderConfig::default();

        let sequential = read_sequential(
            test_file_str,
            vec![String::from("id"), String::from("value")],
            FileFormat::Parquet,
        )
        .unwrap();

        let parallel = read_parallel(
            test_file_str,
            vec![String::from("id"), String::from("value")],
            FileFormat::Parquet,
            &config,
        )
        .unwrap();

        let seq_rows: usize = sequential.iter().map(|b| b.num_rows()).sum();
        let par_rows: usize = parallel.iter().map(|b| b.num_rows()).sum();

        assert_eq!(seq_rows, par_rows);
        assert_eq!(seq_rows, 512_000);
    }

    #[test]
    fn test_streaming_csv_basic() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age").unwrap();
        for i in 0..100 {
            writeln!(file, "Person{},{}", i, 20 + i).unwrap();
        }

        let config = ReaderConfig::default();
        let receiver = read_streaming(
            file.path().to_str().unwrap(),
            vec!["name".to_string(), "age".to_string()],
            FileFormat::Csv,
            config,
        )
        .unwrap();

        let mut total_rows = 0;
        for mini_batch_result in receiver {
            let mini_batch = mini_batch_result.unwrap();
            for batch in mini_batch {
                total_rows += batch.num_rows();
            }
        }

        assert_eq!(total_rows, 100);
    }

    #[test]
    fn test_streaming_parquet_basic() {
        let test_file = get_parquet_test_file();
        let config = ReaderConfig::default();

        let receiver = read_streaming(
            test_file.to_str().unwrap(),
            vec![String::from("id"), String::from("name")],
            FileFormat::Parquet,
            config,
        )
        .unwrap();

        let mut total_rows = 0;
        for mini_batch_result in receiver {
            let mini_batch = mini_batch_result.unwrap();
            for batch in mini_batch {
                total_rows += batch.num_rows();
            }
        }

        assert_eq!(total_rows, 512_000);
    }
}
