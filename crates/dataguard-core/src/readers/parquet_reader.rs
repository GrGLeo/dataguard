use std::{fs::File, sync::Arc};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use rayon::prelude::*;

use crate::readers::BATCH_SIZE;

/// Create a projection mask from column names.
///
/// # Arguments
///
/// * `schema` - The Parquet file schema descriptor
/// * `cols` - Slice of requested column names
///
/// # Returns
///
/// A `ProjectionMask` for the requested columns. If cols is empty, returns an
/// empty projection mask (no columns).
///
/// # Note
///
/// If a requested column is not present in the Parquet schema, it will be silently
/// dismissed without raising an error. Only columns that exist in the file will be
/// included in the projection mask. Empty strings are treated as non-existent
/// column names and will be filtered out.
fn create_projection_mask(schema: &SchemaDescriptor, cols: &[&str]) -> ProjectionMask {
    // Filter out empty strings (treat them as non-existent columns)
    let valid_cols: Vec<&str> = cols.iter().filter(|c| !c.is_empty()).copied().collect();

    if valid_cols.is_empty() {
        // Return empty projection (no columns)
        return ProjectionMask::roots(schema, vec![]);
    }
    ProjectionMask::columns(schema, valid_cols.iter().copied())
}

/// Reads a Parquet file sequentially in a single thread.
///
/// # Arguments
///
/// * `path` - Path to the Parquet file
/// * `cols` - List of column names to read (empty vec returns no columns)
///
/// # Returns
///
/// A vector of Arrow RecordBatches containing the requested columns.
///
/// # Note
///
/// If a requested column is not present in the Parquet file, it will be silently
/// dismissed without raising an error. Only columns that exist in the file will be
/// included in the resulting batches. Empty strings in the column list are treated
/// as non-existent columns and will be filtered out.
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

/// Reads a specific row group from a Parquet file.
///
/// Called by parallel workers to process individual row groups concurrently.
///
/// # Arguments
///
/// * `path` - Path to the Parquet file
/// * `row_group_idx` - Index of the row group to read
/// * `projection` - Projection mask specifying which columns to read
///
/// # Returns
///
/// A vector of Arrow RecordBatches for the specified row group.
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

/// Reads a Parquet file in parallel by processing row groups concurrently.
///
/// # Arguments
///
/// * `path` - Path to the Parquet file
/// * `cols` - List of column names to read (empty vec returns no columns)
///
/// # Returns
///
/// A vector of Arrow RecordBatches containing the requested columns.
///
/// # Note
///
/// If a requested column is not present in the Parquet file, it will be silently
/// dismissed without raising an error. Only columns that exist in the file will be
/// included in the resulting batches. Empty strings in the column list are treated
/// as non-existent columns and will be filtered out.
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

    let batches: Result<Vec<_>, _> = row_group_indices
        .into_par_iter()
        .map(|rg_idx| read_row_group(path, rg_idx, projection.clone()))
        .collect();

    Ok(batches?.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn get_test_file_path() -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("tests");
        path.push("fixtures");
        path.push("test_ecommerce_data.parquet");
        path
    }

    #[test]
    fn test_parquet_sequential_all_columns() {
        let test_file = get_test_file_path();
        let batches = read_parquet_sequential(
            test_file.to_str().unwrap(),
            vec![
                String::from("id"),
                String::from("name"),
                String::from("value"),
            ],
        )
        .unwrap();

        assert!(!batches.is_empty(), "Should have at least one batch");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 512_000, "Should have 512k rows");

        // Check that we have all 3 columns
        assert_eq!(batches[0].num_columns(), 3, "Should have 3 columns");
    }

    #[test]
    fn test_parquet_sequential_specific_columns() {
        let test_file = get_test_file_path();
        let batches = read_parquet_sequential(
            test_file.to_str().unwrap(),
            vec![String::from("id"), String::from("value")],
        )
        .unwrap();

        assert!(!batches.is_empty());

        // Should only have 2 columns
        assert_eq!(batches[0].num_columns(), 2, "Should have 2 columns");

        let schema = batches[0].schema();
        assert!(schema.column_with_name("id").is_some());
        assert!(schema.column_with_name("value").is_some());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 512_000, "Should still have 512k rows");
    }

    #[test]
    fn test_parquet_sequential_single_column() {
        let test_file = get_test_file_path();
        let batches =
            read_parquet_sequential(test_file.to_str().unwrap(), vec![String::from("name")])
                .unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 1, "Should have 1 column");

        let schema = batches[0].schema();
        assert!(schema.column_with_name("name").is_some());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 512_000);
    }

    #[test]
    fn test_parquet_parallel_all_columns() {
        let test_file = get_test_file_path();
        let batches = read_parquet_parallel(
            test_file.to_str().unwrap(),
            vec![
                String::from("id"),
                String::from("name"),
                String::from("value"),
            ],
        )
        .unwrap();

        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 512_000, "Should have 512k rows");

        assert_eq!(batches[0].num_columns(), 3, "Should have 3 columns");
    }

    #[test]
    fn test_parquet_parallel_with_projection() {
        let test_file = get_test_file_path();
        let batches = read_parquet_parallel(
            test_file.to_str().unwrap(),
            vec![String::from("name"), String::from("value")],
        )
        .unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 2, "Should have 2 columns");

        let schema = batches[0].schema();
        assert!(schema.column_with_name("name").is_some());
        assert!(schema.column_with_name("value").is_some());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 512_000);
    }

    #[test]
    fn test_parquet_parallel_vs_sequential() {
        let test_file = get_test_file_path();
        let test_file_str = test_file.to_str().unwrap();
        let sequential = read_parquet_sequential(
            test_file_str,
            vec![String::from("id"), String::from("value")],
        )
        .unwrap();

        let parallel = read_parquet_parallel(
            test_file_str,
            vec![String::from("id"), String::from("value")],
        )
        .unwrap();

        let seq_rows: usize = sequential.iter().map(|b| b.num_rows()).sum();
        let par_rows: usize = parallel.iter().map(|b| b.num_rows()).sum();

        assert_eq!(seq_rows, par_rows, "Should have same number of rows");
        assert_eq!(seq_rows, 512_000);

        assert_eq!(
            sequential[0].schema(),
            parallel[0].schema(),
            "Should have same schema"
        );
    }

    #[test]
    fn test_parquet_empty_projection() {
        let test_file = get_test_file_path();
        let batches = read_parquet_sequential(test_file.to_str().unwrap(), vec![]).unwrap();

        // Empty projection should return batches with 0 columns
        if !batches.is_empty() {
            assert_eq!(batches[0].num_columns(), 0, "Should have 0 columns");
        }
    }

    #[test]
    fn test_parquet_empty_string_filtered() {
        let test_file = get_test_file_path();
        // Empty strings should be filtered out, resulting in empty projection
        let batches =
            read_parquet_sequential(test_file.to_str().unwrap(), vec![String::from("")]).unwrap();

        // Should behave same as empty vec - 0 columns
        if !batches.is_empty() {
            assert_eq!(batches[0].num_columns(), 0, "Should have 0 columns");
        }
    }

    #[test]
    fn test_parquet_mixed_valid_invalid_columns() {
        let test_file = get_test_file_path();
        // Mix of valid columns, invalid columns, and empty strings
        let batches = read_parquet_sequential(
            test_file.to_str().unwrap(),
            vec![
                String::from("id"),
                String::from(""),
                String::from("nonexistent"),
                String::from("value"),
            ],
        )
        .unwrap();

        assert!(!batches.is_empty());
        // Should only have "id" and "value" (empty string and nonexistent filtered out)
        assert_eq!(batches[0].num_columns(), 2, "Should have 2 columns");

        let schema = batches[0].schema();
        assert!(schema.column_with_name("id").is_some());
        assert!(schema.column_with_name("value").is_some());
    }
}
