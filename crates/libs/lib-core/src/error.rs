use std::fmt;

#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "There is an error: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn from_string(error: String) -> Result<(), Box<dyn std::error::Error>> {
        Err(Box::new(Error(error)))
    }
}

