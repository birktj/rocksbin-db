extern crate rocksbin;
extern crate tempfile;

use rocksbin::DB;

#[test]
fn create_db() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let _db = DB::open(dir.path()).expect("open db");
}

#[test]
fn prefix() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    assert!(db.prefix::<u64, u64>(b"test").is_ok());
    assert!(db.prefix::<u64, u64>(b"test1").is_ok());
    assert!(db.prefix::<u64, u64>(b"test2").is_ok());
    assert!(db.prefix::<u64, u64>(b"abc").is_ok());
}

#[test]
fn insert() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix = db.prefix::<u64, u64>(b"test").expect("prefix #1");

    prefix.insert(&5, &7).expect("insert #1");
}

#[test]
fn get() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix = db.prefix::<u64, u64>(b"test").expect("prefix #1");

    prefix.insert(&5, &7).expect("insert #1");
    assert_eq!(prefix.get(&5).expect("get #1"), Some(7));
    assert_eq!(prefix.get(&6).expect("get #2"), None);
}

#[test]
fn remove() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix = db.prefix::<u64, u64>(b"test").expect("prefix #1");

    prefix.insert(&5, &7).expect("insert #1");
    assert_eq!(prefix.get(&5).expect("get #1"), Some(7));
    assert_eq!(prefix.get(&6).expect("get #2"), None);
    prefix.remove(&5).expect("remove #1");
    assert_eq!(prefix.get(&5).expect("get #3"), None);
}

#[test]
fn modify() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix = db.prefix::<u64, u64>(b"test").expect("prefix #1");

    prefix.insert(&5, &7).expect("insert #1");
    assert_eq!(prefix.get(&5).expect("get #1"), Some(7));
    prefix.modify(&5, |val| *val = 8).expect("modify #1");
    assert_eq!(prefix.get(&5).expect("get #3"), Some(8));
}

#[test]
fn multiple_prefix() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix1 = db.prefix::<u64, u64>(b"test").expect("prefix #1");
    let prefix2 = db.prefix::<u64, u64>(b"test2").expect("prefix #1");

    prefix1.insert(&5, &7).expect("insert #1");
    prefix2.insert(&6, &7).expect("insert #2");
    assert_eq!(prefix1.get(&5).expect("get #1"), Some(7));
    assert_eq!(prefix1.get(&6).expect("get #2"), None);
    assert_eq!(prefix2.get(&5).expect("get #3"), None);
    assert_eq!(prefix2.get(&6).expect("get #4"), Some(7));
}

#[test]
fn iter() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix = db.prefix::<u64, u64>(b"test").expect("prefix #1");

    prefix.insert(&5, &7).expect("insert #1");
    prefix.insert(&6, &8).expect("insert #2");
    prefix.insert(&7, &9).expect("insert #3");

    let mut iter = prefix.iter();

    assert_eq!(iter.next().unwrap().unwrap(), (5, 7));
    assert_eq!(iter.next().unwrap().unwrap(), (6, 8));
    assert_eq!(iter.next().unwrap().unwrap(), (7, 9));
    assert!(iter.next().is_none());
}

#[test]
fn iter_multiple_prefix() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix1 = db.prefix::<u64, u64>(b"test").expect("prefix #1");
    let prefix2 = db.prefix::<u64, u64>(b"test2").expect("prefix #1");

    prefix1.insert(&5, &7).expect("insert #1");
    prefix1.insert(&6, &8).expect("insert #2");
    prefix1.insert(&7, &9).expect("insert #3");

    prefix2.insert(&5, &7).expect("insert #4");
    prefix2.insert(&6, &8).expect("insert #5");
    prefix2.insert(&7, &9).expect("insert #6");

    let mut iter1 = prefix2.iter();

    assert_eq!(iter1.next().unwrap().unwrap(), (5, 7));
    assert_eq!(iter1.next().unwrap().unwrap(), (6, 8));
    assert_eq!(iter1.next().unwrap().unwrap(), (7, 9));
    assert!(iter1.next().is_none());

    let mut iter2 = prefix2.iter();

    assert_eq!(iter2.next().unwrap().unwrap(), (5, 7));
    assert_eq!(iter2.next().unwrap().unwrap(), (6, 8));
    assert_eq!(iter2.next().unwrap().unwrap(), (7, 9));
    assert!(iter2.next().is_none());
}

#[test]
fn values() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix = db.prefix::<u64, u64>(b"test").expect("prefix #1");

    prefix.insert(&5, &7).expect("insert #1");
    prefix.insert(&6, &8).expect("insert #2");
    prefix.insert(&7, &9).expect("insert #3");

    let mut iter = prefix.values();

    assert_eq!(iter.next().unwrap().unwrap(), 7);
    assert_eq!(iter.next().unwrap().unwrap(), 8);
    assert_eq!(iter.next().unwrap().unwrap(), 9);
    assert!(iter.next().is_none());
}

#[test]
fn keys() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix = db.prefix::<u64, u64>(b"test").expect("prefix #1");

    prefix.insert(&5, &7).expect("insert #1");
    prefix.insert(&6, &8).expect("insert #2");
    prefix.insert(&7, &9).expect("insert #3");

    let mut iter = prefix.keys();

    assert_eq!(iter.next().unwrap().unwrap(), 5);
    assert_eq!(iter.next().unwrap().unwrap(), 6);
    assert_eq!(iter.next().unwrap().unwrap(), 7);
    assert!(iter.next().is_none());
}

#[test]
fn prefix_group() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix1 = db.prefix::<u64, u64>(b"test").expect("prefix #1");
    let prefix_group = db.prefix_group(b"test2").expect("prefix group #1");
    let prefix2 = prefix_group
        .prefix::<u64, u64>(b"test2")
        .expect("prefix #2");

    prefix1.insert(&5, &7).expect("insert #1");
    prefix2.insert(&5, &9).expect("insert #2");

    assert_eq!(prefix1.get(&5).unwrap(), Some(7));
    assert_eq!(prefix2.get(&5).unwrap(), Some(9));
}

#[test]
fn sub_prefix_group() {
    let dir = tempfile::tempdir().expect("create tempdir");
    let db = DB::open(dir.path()).expect("open db");
    let prefix1 = db.prefix::<u64, u64>(b"test").expect("prefix #1");
    let prefix_group1 = db.prefix_group(b"test2").expect("prefix group #1");
    let prefix_group2 = db.prefix_group(b"test3").expect("prefix group #2");
    let prefix2 = prefix_group1
        .prefix::<u64, u64>(b"test2")
        .expect("prefix #2");
    let prefix3 = prefix_group2
        .prefix::<u64, u64>(b"test2")
        .expect("prefix #3");

    prefix1.insert(&5, &7).expect("insert #1");
    prefix2.insert(&5, &9).expect("insert #2");
    prefix3.insert(&5, &11).expect("insert #3");

    assert_eq!(prefix1.get(&5).unwrap(), Some(7));
    assert_eq!(prefix2.get(&5).unwrap(), Some(9));
    assert_eq!(prefix3.get(&5).unwrap(), Some(11));
}
