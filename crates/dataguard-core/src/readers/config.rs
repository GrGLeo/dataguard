#![allow(dead_code)]

pub struct ReaderConfig {
    min_chunk_size: u64,
    max_chunk_size: u64,
    thread_per_chunk: u8,
    batch_size: u32,
    streaming: bool,
    streaming_threshold: u64,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            min_chunk_size: 1024 * 1024,
            max_chunk_size: 100 * 1024 * 1024,
            thread_per_chunk: 4,
            batch_size: 26 * 1024,
            streaming: false,
            streaming_threshold: 500 * 1024 * 1024,
        }
    }
}

pub struct ReaderConfigBuilder {
    min_chunk_size: u64,
    max_chunk_size: u64,
    thread_per_chunk: u8,
    batch_size: u32,
    streaming: bool,
    streaming_threshold: u64,
}

impl ReaderConfigBuilder {
    /// Create a new [`ReaderConfigBuilder`]
    pub fn new() -> Self {
        let reader = ReaderConfig::default();
        Self {
            min_chunk_size: reader.min_chunk_size,
            max_chunk_size: reader.max_chunk_size,
            thread_per_chunk: reader.thread_per_chunk,
            batch_size: reader.batch_size,
            streaming: reader.streaming,
            streaming_threshold: reader.streaming_threshold,
        }
    }

    /// Build a [`ReaderConfig`]
    pub fn build(self) -> ReaderConfig {
        ReaderConfig {
            min_chunk_size: self.min_chunk_size,
            max_chunk_size: self.max_chunk_size,
            thread_per_chunk: self.thread_per_chunk,
            batch_size: self.batch_size,
            streaming: self.streaming,
            streaming_threshold: self.streaming_threshold,
        }
    }

    pub fn with_min_chunk_size(self, min: u64) -> Self {
        Self {
            min_chunk_size: min,
            ..self
        }
    }
    pub fn with_max_chung_size(self, max: u64) -> Self {
        Self {
            max_chunk_size: max,
            ..self
        }
    }
    pub fn with_thread_per_chunk(self, thread: u8) -> Self {
        Self {
            thread_per_chunk: thread,
            ..self
        }
    }
    pub fn with_batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
    }

    pub fn with_streaming(self, stream: bool) -> Self {
        Self {
            streaming: stream,
            ..self
        }
    }

    pub fn with_streaming_threshold(self, threshold: u64) -> Self {
        Self {
            streaming_threshold: threshold,
            ..self
        }
    }
}

impl ReaderConfig {
    pub fn should_stream(&self, file_size: u64) -> bool {
        self.streaming || file_size >= self.streaming_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_default() {
        let reader = ReaderConfig::default();
        assert_eq!(reader.min_chunk_size, 1048576);
        assert_eq!(reader.max_chunk_size, 104857600);
        assert_eq!(reader.thread_per_chunk, 4);
        assert_eq!(reader.batch_size, 26 * 1024);
        assert_eq!(reader.streaming, false);
        assert_eq!(reader.streaming_threshold, 500 * 1024 * 1024);
    }

    #[test]
    fn test_reader_streaming() {
        let reader = ReaderConfig::default();
        let st = reader.should_stream(1 * 1024 * 1024 * 1024); // 1G should stream
        assert!(st)
    }

    #[test]
    fn test_reader_builder() {
        let reader_builder = ReaderConfigBuilder::new();
        let reader_builder = reader_builder
            .with_batch_size(128_000)
            .with_streaming_threshold(100 * 1024 * 1024)
            .with_min_chunk_size(2 * 1024 * 1024);
        let reader = reader_builder.build();
        assert_eq!(reader.batch_size, 128_000);
        assert_eq!(reader.streaming_threshold, 100 * 1024 * 1024);
        assert_eq!(reader.min_chunk_size, 2 * 1024 * 1024);
    }
}
