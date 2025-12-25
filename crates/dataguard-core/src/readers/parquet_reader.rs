use std::{fs::File, sync::Arc};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use rayon::prelude::*;

use crate::readers::BATCH_SIZE;

/// Create a projection mask from column names
/// If cols is empty or contains only empty strings, returns all columns
fn create_projection_mask(schema: &SchemaDescriptor, cols: &[&str]) -> ProjectionMask {
    if cols.is_empty() || (cols.len() == 1 && cols[0].is_empty()) {
        return ProjectionMask::all();
    }
    ProjectionMask::columns(schema, cols.iter().copied())
}

pub fn read_parquet_sequential(
    path: &str,
    cols: Vec<String>,
) -> Result<Vec<Arc<RecordBatch>>, std::io::Error> {
    let cols: Vec<&str> = cols.iter().map(|v| v.as_str()).collect();
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let projection = create_projection_mask(builder.parquet_schema(), cols.as_slice());

    // Build reader with projection and batch size
    let reader = builder
        .with_projection(projection)
        .with_batch_size(BATCH_SIZE)
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch =
            batch_result.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }
    Ok(batches)
}

/// Read a specific row group from a parquet file
/// Called by parallel workers
fn read_row_group(
    path: &str,
    row_group_idx: usize,
    projection: ProjectionMask,
) -> Result<Vec<Arc<RecordBatch>>, std::io::Error> {
    // Each thread opens its own file handle
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Build reader for specific row group with projection
    let reader = builder
        .with_projection(projection)
        .with_row_groups(vec![row_group_idx])
        .with_batch_size(BATCH_SIZE)
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch =
            batch_result.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        batches.push(Arc::new(batch));
    }
    Ok(batches)
}

/// Read parquet file in parallel by processing row groups concurrently
pub fn read_parquet_parallel(
    path: &str,
    cols: Vec<String>,
) -> Result<Vec<Arc<RecordBatch>>, std::io::Error> {
    let cols: Vec<&str> = cols.iter().map(|v| v.as_str()).collect();

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Get row group count and create projection
    let num_row_groups = builder.metadata().num_row_groups();
    let projection = create_projection_mask(builder.parquet_schema(), cols.as_slice());
    let row_group_indices: Vec<usize> = (0..num_row_groups).collect();

    // Process row groups in parallel using rayon
    let batches: Result<Vec<_>, _> = row_group_indices
        .into_par_iter()
        .map(|rg_idx| read_row_group(path, rg_idx, projection.clone()))
        .collect();

    // Flatten and return (fail fast on error)
    Ok(batches?.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_FILE: &str = "/home/leo/code/dataguard/benchmark/ecommerce_data.parquet";

    #[test]
    fn test_parquet_sequential_all_columns() {
        let batches = read_parquet_sequential(TEST_FILE, vec![String::from("")]).unwrap();

        assert!(!batches.is_empty(), "Should have at least one batch");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2_000_000, "Should have 2M rows");

        // Check that we have all 8 columns
        assert_eq!(batches[0].num_columns(), 8, "Should have 8 columns");
    }

    #[test]
    fn test_parquet_sequential_specific_columns() {
        let batches = read_parquet_sequential(
            TEST_FILE,
            vec![
                String::from("customer_id"),
                String::from("product_id"),
                String::from("product_price"),
            ],
        )
        .unwrap();

        assert!(!batches.is_empty());

        // Should only have 3 columns
        assert_eq!(batches[0].num_columns(), 3, "Should have 3 columns");

        let schema = batches[0].schema();
        assert!(schema.column_with_name("customer_id").is_some());
        assert!(schema.column_with_name("product_id").is_some());
        assert!(schema.column_with_name("product_price").is_some());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2_000_000, "Should still have 2M rows");
    }

    #[test]
    fn test_parquet_sequential_single_column() {
        let batches =
            read_parquet_sequential(TEST_FILE, vec![String::from("customer_name")]).unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 1, "Should have 1 column");

        let schema = batches[0].schema();
        assert!(schema.column_with_name("customer_name").is_some());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2_000_000);
    }

    #[test]
    fn test_parquet_parallel_all_columns() {
        let batches = read_parquet_parallel(TEST_FILE, vec![String::from("")]).unwrap();

        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2_000_000, "Should have 2M rows");

        assert_eq!(batches[0].num_columns(), 8, "Should have 8 columns");
    }

    #[test]
    fn test_parquet_parallel_with_projection() {
        let batches =
            read_parquet_parallel(TEST_FILE, vec![String::from("email"), String::from("sexe")])
                .unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 2, "Should have 2 columns");

        let schema = batches[0].schema();
        assert!(schema.column_with_name("email").is_some());
        assert!(schema.column_with_name("sexe").is_some());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2_000_000);
    }

    #[test]
    fn test_parquet_parallel_vs_sequential() {
        let sequential = read_parquet_sequential(
            TEST_FILE,
            vec![String::from("product_id"), String::from("product_price")],
        )
        .unwrap();

        let parallel = read_parquet_parallel(
            TEST_FILE,
            vec![String::from("product_id"), String::from("product_price")],
        )
        .unwrap();

        let seq_rows: usize = sequential.iter().map(|b| b.num_rows()).sum();
        let par_rows: usize = parallel.iter().map(|b| b.num_rows()).sum();

        assert_eq!(seq_rows, par_rows, "Should have same number of rows");
        assert_eq!(seq_rows, 2_000_000);

        assert_eq!(
            sequential[0].schema(),
            parallel[0].schema(),
            "Should have same schema"
        );
    }
}
