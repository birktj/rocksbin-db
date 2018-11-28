# Rocksbin-db
[![Crates.io](https://img.shields.io/crates/v/rocksbin.svg)](https://crates.io/crates/rocksbin)
[![Build Status](https://img.shields.io/travis/com/birktj/rocksbin-db.svg)](https://travis-ci.com/birktj/rocksbin-db)
[![Documentation](https://docs.rs/rocksbin/badge.svg)](https://docs.rs/rocksbin)
[![GitHub license](https://img.shields.io/github/license/birktj/rocksbin-db.svg)](https://github.com/birktj/rocksbin-db/blob/master/LICENSE)

A simple rust rocksdb wrapper using serde and bincode
for automatic serialization.

This library is perfect if what you want is a persistent
HashMap stored on disk and a simple API.

```rust
extern crate rocksbin_db;

use rocksbin_db::DB;

let db = DB::open("db_dir").unwrap();

let fish_count = db.prefix::<String, u64>(b"fish_count").unwarp();

fish_count.insert(&"salmon".to_string(), 10).unwarp();
fish_count.insert(&"cod".to_string(), 100).unwarp();
fish_count.insert(&"mackerel".to_string(), 70).unwarp();

assert_eq!(fish_count.get(&"salmon".to_string()).unwarp(), Some(10));
```
