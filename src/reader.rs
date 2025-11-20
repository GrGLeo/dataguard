use std::{
    fs::File,
    io::{self, BufRead, BufReader, Error},
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    csv::ReaderBuilder,
    datatypes::{DataType, Field, Schema},
};

const BATCH: usize = 64_000;

pub fn read_csv(path: &str) -> Result<Vec<Arc<RecordBatch>>, Error> {
    let file = File::open(path)?;
    // For now we treat every column as utf8
    let res = generate_utf_schema(path);
    let mut batches: Vec<Arc<RecordBatch>> = Vec::new();
    match res {
        Ok(schema) => {
            if let Ok(reader) = ReaderBuilder::new(Arc::new(schema))
                .with_header(true)
                .with_batch_size(BATCH)
                .build(file)
            {
                for batch in reader {
                    let batch = batch.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    batches.push(Arc::new(batch));
                }
                Ok(batches)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CSV file is empty",
                ))
            }
        }
        Err(e) => Err(e),
    }
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
    fn test_read_csv_valid() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,age").unwrap();
        writeln!(file, "Alice,30").unwrap();
        writeln!(file, "Bob,25").unwrap();

        let batches = read_csv(file.path().to_str().unwrap()).unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_read_csv_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let result = read_csv(file.path().to_str().unwrap());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_read_csv_invalid_path() {
        let result = read_csv("nonexistent.csv");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }
}
