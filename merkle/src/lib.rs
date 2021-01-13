#![feature(const_fn)]
#![feature(btree_drain_filter)]

use failure::Fail;

mod hash;
mod blake2b;
mod base58;
mod schema;
mod codec;
mod  merkle_storage;
mod database;
mod db_iterator;
mod ivec;
mod crypto_box;
mod nonce;

pub mod prelude {
    pub use crate::database::*;
    pub use crate::merkle_storage::*;
    pub use crate::db_iterator::*;
    pub use crate::codec::*;
    pub use crate::hash::*;
    pub use crate::ivec::IVec;
    pub use crate::crypto_box::*;
    pub use crate::nonce::*;
}

#[derive(Debug, Fail)]
pub enum CryptoError {
    #[fail(display = "Invalid crypto key, reason: {}", reason)]
    InvalidKey { reason: String },
    #[fail(
    display = "Invalid crypto key size - expected: {}, actual: {}",
    expected, actual
    )]
    InvalidKeySize { expected: usize, actual: usize },
    #[fail(
    display = "Invalid nonce size - expected: {}, actual: {}",
    expected, actual
    )]
    InvalidNonceSize { expected: usize, actual: usize },
    #[fail(display = "Failed to decrypt")]
    FailedToDecrypt,
}
