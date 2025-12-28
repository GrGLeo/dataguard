mod config;
pub mod csv_reader;
pub mod parquet_reader;

pub use parquet_reader::read_parquet_parallel;
pub use parquet_reader::read_parquet_sequential;

const BATCH_SIZE: usize = 256_000;
