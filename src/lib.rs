extern crate rocksdb;
extern crate bincode;
extern crate serde;

use serde::{Serialize, de::DeserializeOwned};

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::marker::PhantomData;
use std::path::Path;
use std::fmt;
use std::error;

#[derive(Debug)]
pub enum ErrorKind {
    Bincode(bincode::Error),
    Rocksdb(rocksdb::Error),
    PrefixExists,
}

pub type Error = Box<ErrorKind>;
pub type Result<T> = ::std::result::Result<T, Error>;

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
            ErrorKind::PrefixExists => write!(f, "prefix exists"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match **self {
            ErrorKind::Bincode(ref e) => Some(e),
            ErrorKind::Rocksdb(ref e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct DB {
    db: Arc<rocksdb::DB>,
    prefix_list: Arc<Mutex<BTreeSet<Vec<u8>>>>,
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<DB> {
        Ok(DB {
            db: Arc::new(rocksdb::DB::open_default(path)?),
            prefix_list: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    pub fn prefix<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned>(&self, prefix: &[u8]) -> Result<Prefix<K, V>> {
        let prefix = prefix.to_owned();
        
        if self.prefix_list.lock()
            .unwrap() // TODO: add extra errorkind instead
            .range(prefix.clone()..) // TODO: optimize
            .next()
            .map(|p| p.starts_with(&prefix))
            .unwrap_or(false) {
                return Err(Box::new(ErrorKind::PrefixExists));
            }

        if self.prefix_list.lock()
            .unwrap() // TODO: add extra errorkind instead
            .range(..prefix.clone()) // TODO: optimize
            .next_back()
            .map(|p| prefix.starts_with(&p))
            .unwrap_or(false) {
                return Err(Box::new(ErrorKind::PrefixExists));
            }

        self.prefix_list.lock()
            .unwrap()
            .insert(prefix.clone());

        Ok(Prefix {
            db: self.db.clone(),
            prefix: prefix,
            _k: PhantomData,
            _v: PhantomData,
        })
    }
}

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
