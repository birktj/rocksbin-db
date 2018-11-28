# Rocksbin-db

A simple rust rocksdb wrapper using serde and bincode
for automatic serialization.

This library is perfect if what you really want is a
persistent HashMap without having to keep it in memory.

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