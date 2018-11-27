//! `rocksbin-db` is a simple library wrapping rocksdb in 
//! an interface mimicing rust collections like `HashMap`.
//!
//! It does this by utilising serde and bincode to automaticly
//! serialize data you enter into the database.
//!
//! # Examples
//!
//! ```
//! #[macro_use]
//! extern crate serde_derive;
//!
//! #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
//! struct Fish {
//!     count: u64,
//!     latin_name: String,
//! }
//!
//! # fn main() {
//! let db = rocksbin::DB::open("db_dir").unwrap();
//!
//! let fish = db.prefix::<String, Fish>(b"fish").unwrap();
//!
//! let salmon = Fish {
//!     count: 100,
//!     latin_name: "Salmo salar".to_string(),
//! };
//!
//! fish.insert(&"salmon".to_string(), &salmon);
//!
//! assert_eq!(fish.get(&"salmon".to_string()).unwrap(), Some(salmon));
//!
//! # drop(fish);
//! # drop(db);
//! # std::fs::remove_dir_all("db_dir").unwrap();
//! # }
//! ```

extern crate rocksdb;
extern crate bincode;
extern crate serde;

use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;
use std::marker::PhantomData;
use std::path::Path;
use std::fmt;
use std::error;

#[derive(Debug)]
pub enum ErrorKind {
    Bincode(bincode::Error),
    Rocksdb(rocksdb::Error),
}

pub type Error = Box<ErrorKind>;

type Result<T> = ::std::result::Result<T, Error>;

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Error {
        Box::new(ErrorKind::Bincode(e))
    }
}

impl From<rocksdb::Error> for Error {
    fn from(e: rocksdb::Error) -> Error {
        Box::new(ErrorKind::Rocksdb(e))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match **self {
            ErrorKind::Bincode(ref e) => write!(f, "bincode error: {}", e),
            ErrorKind::Rocksdb(ref e) => write!(f, "rocksdb error: {}", e),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match **self {
            ErrorKind::Bincode(ref e) => Some(e),
            ErrorKind::Rocksdb(ref e) => Some(e),
        }
    }
}

/// Entry point for this library
#[derive(Clone)]
pub struct DB {
    db: Arc<rocksdb::DB>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        Ok(DB {
            db: Arc::new(rocksdb::DB::open_default(path)?),
        })
    }

    pub fn prefix<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned>(&self, prefix: &[u8]) -> Result<Prefix<K, V>> {
        // No point in using 64bit lenght here
        // This will never fail
        let mut prefix_vec = bincode::serialize(&(prefix.len() as u32)).unwrap();
        prefix_vec.extend_from_slice(&prefix);

        Ok(Prefix {
            db: self.db.clone(),
            prefix: prefix_vec,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    /// Create a prefix group
    ///
    /// It is important that a `PrefixGroup` never has the same prefix as `Prefix`, if they do you
    /// might get key parse errors
    pub fn prefix_group(&self, prefix: &[u8]) -> Result<PrefixGroup> {
        // No point in using 64bit lenght here
        // This will never fail
        let mut prefix_vec = bincode::serialize(&(prefix.len() as u32)).unwrap();
        prefix_vec.extend_from_slice(&prefix);

        Ok(PrefixGroup {
            db: self.db.clone(),
            prefix: prefix_vec,
        })
    }
}

/// A way to group prefixes
#[derive(Clone)]
pub struct PrefixGroup {
    db: Arc<rocksdb::DB>,
    prefix: Vec<u8>,
}

impl PrefixGroup {
    /// Create a prefix inside this prefix group
    ///
    /// See `DB::prefix`
    pub fn prefix<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned>(&self, prefix: &[u8]) -> Result<Prefix<K, V>> {
        // No point in using 64bit lenght here
        // This will never fail
        let mut prefix_vec = self.prefix.clone();
        bincode::serialize_into(&mut prefix_vec, &(prefix.len() as u32))?;
        prefix_vec.extend_from_slice(&prefix);

        Ok(Prefix {
            db: self.db.clone(),
            prefix: prefix_vec,
            _k: PhantomData,
            _v: PhantomData,
        })
    }

    /// Create a sub prefix group
    ///
    /// See `DB::prefix_group`
    pub fn prefix_group(&self, prefix: &[u8]) -> Result<PrefixGroup> {
        // No point in using 64bit lenght here
        // This will never fail
        let mut prefix_vec = self.prefix.clone();
        bincode::serialize_into(&mut prefix_vec, &(prefix.len() as u32))?;
        prefix_vec.extend_from_slice(&prefix);

        Ok(PrefixGroup {
            db: self.db.clone(),
            prefix: prefix_vec,
        })
    }
    
}

/// A entry point to data stored in the database 
#[derive(Clone)]
pub struct Prefix<K, V> {
    db: Arc<rocksdb::DB>,
    prefix: Vec<u8>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Prefix<K, V> {
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let mut key_buf = self.prefix.clone();
        key_buf.reserve(bincode::serialized_size(&key)? as usize);
        bincode::serialize_into(&mut key_buf, &key)?;
        match self.db.get(&key_buf)? {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub fn insert(&self, key: &K, value: &V) -> Result<()> {
        let mut key_buf = self.prefix.clone();
        key_buf.reserve(bincode::serialized_size(&key)? as usize);
        bincode::serialize_into(&mut key_buf, &key)?;
        let value_buf = bincode::serialize(value)?;

        self.db.put(&key_buf, &value_buf)?;
        Ok(())
    }

    pub fn remove(&self, key: &K) -> Result<()> {
        let mut key_buf = self.prefix.clone();
        key_buf.reserve(bincode::serialized_size(&key)? as usize);
        bincode::serialize_into(&mut key_buf, &key)?;

        self.db.delete(&key_buf)?;
        Ok(())
    }

    pub fn contains_key(&self, key: &K) -> Result<bool> {
        self.get(key).map(|v| v.is_some()) // TODO: optimize
    }

    pub fn modify<F: FnOnce(&mut V)>(&self, key: &K, f: F) -> Result<()> {
        match self.get(key)? {
            Some(mut value) => {
                f(&mut value);
                self.insert(&key, &value)
            }
            None => Ok(())
        }
    }

    pub fn iter(&self) -> Iter<K, V> {
        let mut db_iter = self.db.raw_iterator();
        db_iter.seek(&self.prefix);

        Iter {
            db_iter,
            prefix: self.prefix.clone(),
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    pub fn keys(&self) -> Keys<K> {
        let mut db_iter = self.db.raw_iterator();
        db_iter.seek(&self.prefix);

        Keys {
            db_iter,
            prefix: self.prefix.clone(),
            _k: PhantomData,
        }
    }

    pub fn values(&self) -> Values<V> {
        let mut db_iter = self.db.raw_iterator();
        db_iter.seek(&self.prefix);

        Values {
            db_iter,
            prefix: self.prefix.clone(),
            _v: PhantomData,
        }
    }
}

pub struct Iter<K, V> {
    db_iter: rocksdb::DBRawIterator,
    prefix: Vec<u8>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K: DeserializeOwned, V: DeserializeOwned> Iterator for Iter<K, V> {
    type Item = Result<(K, V)>; // :(

    fn next(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let k =
                // We do not reuse the buffer so this is safe
                unsafe {self.db_iter.key_inner()}
                    .and_then(|k| if &k[0..self.prefix.len()] == &self.prefix[..] { Some(k) } else { None } )
                    .map(|k| bincode::deserialize(&k[self.prefix.len()..]));
            let v =
                // We do not reuse the buffer so this is safe
                unsafe {self.db_iter.value_inner()}
                    .map(|k| bincode::deserialize(k));

            self.db_iter.next();
            k.and_then(|k| v.map(|v| Ok((k?, v?))))
        }
        else {
            None
        }
    }
}

pub struct Keys<K> {
    db_iter: rocksdb::DBRawIterator,
    prefix: Vec<u8>,
    _k: PhantomData<K>,
}

impl<K: DeserializeOwned> Iterator for Keys<K> {
    type Item = Result<K>; // :(

    fn next(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let k =
                // We do not reuse the buffer so this is safe
                unsafe {self.db_iter.key_inner()}
                    .and_then(|k| if &k[0..self.prefix.len()] == &self.prefix[..] { Some(k) } else { None } )
                    .map(|k| Ok(bincode::deserialize(&k[self.prefix.len()..])?));

            self.db_iter.next();
            k
        }
        else {
            None
        }
    }
}

pub struct Values<V> {
    db_iter: rocksdb::DBRawIterator,
    prefix: Vec<u8>,
    _v: PhantomData<V>,
}

impl<V: DeserializeOwned> Iterator for Values<V> {
    type Item = Result<V>; // :(

    fn next(&mut self) -> Option<Self::Item> {
        if self.db_iter.valid() {
            let v =
                // We do not reuse the buffer so this is safe
                unsafe {self.db_iter.key_inner()}
                    .and_then(|k| if &k[0..self.prefix.len()] == &self.prefix[..] { Some(k) } else { None } )
                    .and_then(|_| 
                        unsafe {self.db_iter.value_inner()}
                            .map(|v| Ok(bincode::deserialize(v)?))
                        );

            self.db_iter.next();
            v
        }
        else {
            None
        }
    }
}
