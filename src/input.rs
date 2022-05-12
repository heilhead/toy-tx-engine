use crate::transaction::RawTransactionData;
use std::fs;
use std::io;
use std::path::Path;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum InputStreamError {
    #[error(transparent)]
    IoError(#[from] io::Error),

    #[error(transparent)]
    CsvError(#[from] csv::Error),
}

/// A wrapper around `csv::Reader` to lose type parameters.
pub struct InputStream {
    /// Boxed to potentially handle reading from other `std::io::Read` streams, e.g. `std::net::TcpStream`.
    csv_reader: Box<dyn Iterator<Item = csv::Result<RawTransactionData>>>,
}

impl InputStream {
    pub fn from_file<P: AsRef<Path>>(input_path: P) -> Result<Self, InputStreamError> {
        Self::from_reader(fs::File::open(input_path)?)
    }

    pub fn from_reader<R: 'static + io::Read>(reader: R) -> Result<Self, InputStreamError> {
        let reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .from_reader(reader);

        Ok(Self {
            csv_reader: Box::new(reader.into_deserialize()),
        })
    }
}

impl Iterator for InputStream {
    type Item = Result<RawTransactionData, InputStreamError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.csv_reader
            .next()
            .map(|res| res.map_err(InputStreamError::from))
    }
}
