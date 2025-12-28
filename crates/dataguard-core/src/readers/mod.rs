pub mod config;
pub mod reader;

pub use config::ReaderConfig;
pub use reader::{
    read_parallel, read_parallel_auto, read_sequential, read_sequential_auto, read_streaming,
    read_streaming_auto, FileFormat,
};
