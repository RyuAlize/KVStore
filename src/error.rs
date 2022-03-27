use thiserror::Error;
use bincode;
use std::io;
#[derive(Error, Debug)]
pub enum Error{
    #[error("Key not found")]
    KeyNotFound,
    #[error("")]
    PageSizeNotEnough,
    #[error("Page not found")]
    PageNotFound,
    #[error("file open error")]
    IOError(#[from] io::Error),
    #[error("")]
    SerdeError(#[from] Box<bincode::ErrorKind>),
    #[error("Unexpected node type")]
    UnkonwNodeType,
    #[error("roo page ptr is null ")]
    RootPageIsNull
}

pub type Result<T> = std::result::Result<T, Error>;