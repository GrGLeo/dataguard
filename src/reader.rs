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
