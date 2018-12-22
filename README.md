# Rocksbin-db
[![Build Status](https://img.shields.io/travis/com/birktj/rocksbin-db.svg)](https://travis-ci.com/birktj/rocksbin-db)
[![Crates.io](https://img.shields.io/crates/v/rocksbin.svg)](https://crates.io/crates/rocksbin)
[![Documentation](https://docs.rs/rocksbin/badge.svg)](https://docs.rs/rocksbin)
[![GitHub license](https://img.shields.io/github/license/birktj/rocksbin-db.svg)](https://github.com/birktj/rocksbin-db/blob/master/LICENSE)

A simple rust rocksdb wrapper using serde and bincode
for automatic serialization.

This library is perfect if what you want is a persistent
HashMap stored on disk and a simple API.

```rust
extern crate rocksbin;

use rocksbin::DB;

let db = DB::open("db_dir").unwrap();

let fish_count = db.prefix::<String, u64>(b"fish_count").unwarp();

fish_count.insert("salmon", 10).unwarp();
fish_count.insert("cod", 100).unwarp();
fish_count.insert("mackerel", 70).unwarp();

assert_eq!(fish_count.get("salmon").unwarp(), Some(10));
```
