use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

const BATCH: usize = 256_000; // Increased from 256K
const MIN_CHUNK_SIZE: u64 = 50 * 1024 * 1024; // 50MB minimum per chunk

pub fn read_csv_parallel(path: &str) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();

    let schema = Arc::new(generate_utf_schema(path)?);

    let mut header_reader = BufReader::new(File::open(path)?);
    let mut header = String::new();
    header_reader.read_line(&mut header)?;
    let header_len = header.len() as u64;

    let num_threads = rayon::current_num_threads();
    let data_size = file_size - header_len;
    let chunk_size = (data_size / num_threads as u64).max(MIN_CHUNK_SIZE);

    let chunks = create_chunks(path, header_len, file_size, chunk_size)?;

    let batches: Result<Vec<_>, _> = chunks
        .into_par_iter()
        .map(|(start, end)| parse_chunk(path, &schema, &header, start, end))
        .collect();

    Ok(batches?.into_iter().flatten().collect())
}

fn create_chunks(
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
            find_next_newline(&mut file, target_end)?
        };

        chunks.push((current, actual_end));
        current = actual_end;
    }

    Ok(chunks)
}

fn find_next_newline(file: &mut File, pos: u64) -> Result<u64, io::Error> {
    file.seek(SeekFrom::Start(pos))?;
    let mut reader = BufReader::new(file.try_clone()?);
    let mut offset = 0u64;
    let mut buffer = Vec::new();

    reader.read_until(b'\n', &mut buffer)?;
    offset += buffer.len() as u64;

    Ok(pos + offset)
}

fn parse_chunk(
    path: &str,
    schema: &Arc<Schema>,
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
    let reader = ReaderBuilder::new(schema.clone())
        .with_header(true)
        .with_batch_size(BATCH)
        .build(cursor)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }

    Ok(batches)
}

pub fn read_csv_sequential(path: &str) -> Result<Vec<Arc<RecordBatch>>, io::Error> {
    let file = File::open(path)?;
    let schema = Arc::new(generate_utf_schema(path)?);
    let mut batches = Vec::new();

    let reader = ReaderBuilder::new(schema)
        .with_header(true)
        .with_batch_size(BATCH)
        .build(file)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }

    Ok(batches)
}

fn generate_utf_schema(path: &str) -> Result<Schema, io::Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    if let Some(first) = lines.next() {
        let header = first?;
        let cols: Vec<&str> = header.split(',').collect();
        let fields: Vec<Field> = cols
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_generate_utf_schema_valid() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age,city").unwrap();
        writeln!(file, "Alice,30,New York").unwrap();

        let schema = generate_utf_schema(file.path().to_str().unwrap()).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(2).name(), "city");
    }

    #[test]
    fn test_generate_utf_schema_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let result = generate_utf_schema(file.path().to_str().unwrap());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_csv_sequential_valid() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age").unwrap();
        writeln!(file, "Alice,30").unwrap();
        writeln!(file, "Bob,25").unwrap();

        let batches = read_csv_sequential(file.path().to_str().unwrap()).unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_read_csv_sequential_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let result = read_csv_sequential(file.path().to_str().unwrap());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_csv_sequential_invalid_path() {
        let result = read_csv_sequential("nonexistent.csv");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
}
