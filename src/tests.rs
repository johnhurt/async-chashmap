// Copyright 2014-2015 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::cell::RefCell;
use std::sync::Arc;
use std::thread;
use crate::CHashMap;
use futures::executor::block_on;

use tokio::runtime::Builder;

use std::time::SystemTime;

#[test]
fn spam_insert() {
    let m = Arc::new(CHashMap::new());
    let mut joins = Vec::new();

    for t in 0..10 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 1000..(t + 1) * 1000 {
                assert!(block_on(m.insert(i, !i)).is_none());
                assert_eq!(block_on(m.insert(i, i)).unwrap(), !i);
            }
        }));
    }

    for j in joins.drain(..) {
        j.join().unwrap();
    }

    for t in 0..5 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 2000..(t + 1) * 2000 {
                block_on(m.with(&i, |r| assert_eq!(*r.unwrap(), i)));
            }
        }));
    }

    for j in joins {
        j.join().unwrap();
    }
}

#[test]
fn spam_insert_new() {
    let m = Arc::new(CHashMap::new());
    let mut joins = Vec::new();

    for t in 0..10 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 1000..(t + 1) * 1000 {
                block_on(m.insert_new(i, i));
            }
        }));
    }

    for j in joins.drain(..) {
        j.join().unwrap();
    }

    for t in 0..5 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 2000..(t + 1) * 2000 {
                block_on(m.with(&i, |r| assert_eq!(*r.unwrap(), i)));
            }
        }));
    }

    for j in joins {
        j.join().unwrap();
    }
}

#[test]
fn spam_upsert() {
    let m = Arc::new(CHashMap::new());
    let mut joins = Vec::new();

    for t in 0..10 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 1000..(t + 1) * 1000 {
                block_on(m.upsert(i, || !i, |_| unreachable!()));
                block_on(m.upsert(i, || unreachable!(), |x| *x = !*x));
            }
        }));
    }

    for j in joins.drain(..) {
        j.join().unwrap();
    }

    for t in 0..5 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 2000..(t + 1) * 2000 {
                block_on(m.with(&i, |r| assert_eq!(*r.unwrap(), i)));
            }
        }));
    }

    for j in joins {
        j.join().unwrap();
    }
}

#[test]
fn spam_alter() {
    let m = Arc::new(CHashMap::new());
    let mut joins = Vec::new();

    for t in 0..10 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 1000..(t + 1) * 1000 {
                block_on(m.alter(i, |x| {
                    assert!(x.is_none());
                    Some(!i)
                }));
                block_on(m.alter(i, |x| {
                    assert_eq!(x, Some(!i));
                    Some(!x.unwrap())
                }));
            }
        }));
    }

    for j in joins.drain(..) {
        j.join().unwrap();
    }

    for t in 0..5 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            for i in t * 2000..(t + 1) * 2000 {
                block_on(m.with(&i, |r| assert_eq!(*r.unwrap(), i)));
                block_on(m.alter(i, |_| None));
                assert!(block_on(m.with(&i, |r| r.is_none())));
            }
        }));
    }

    for j in joins {
        j.join().unwrap();
    }
}

#[test]
fn lock_compete() {
    let m = Arc::new(CHashMap::new());

    block_on(m.insert("hey", "nah"));

    let k = m.clone();
    let a = thread::spawn(move || {
        block_on(k.with_mut(&"hey", |r| *r.unwrap() = "hi"));
    });
    let k = m.clone();
    let b = thread::spawn(move || {
        block_on(k.with_mut(&"hey", |r| *r.unwrap() = "hi"));
    });

    a.join().unwrap();
    b.join().unwrap();

    block_on(m.with(&"hey", |r| assert_eq!(*r.unwrap(), "hi")));
}

#[test]
fn simultanous_reserve() {
    let m = Arc::new(CHashMap::new());
    let mut joins = Vec::new();

    block_on(m.insert(1, 2));
    block_on(m.insert(3, 6));
    block_on(m.insert(8, 16));

    for _ in 0..10 {
        let m = m.clone();
        joins.push(thread::spawn(move || {
            block_on(m.reserve(1000));
        }));
    }

    for j in joins {
        j.join().unwrap();
    }

    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    block_on(m.with(&3, |r| assert_eq!(*r.unwrap(), 6)));
    block_on(m.with(&8, |r| assert_eq!(*r.unwrap(), 16)));
}


#[test]
fn create_capacity_zero() {
    let m = CHashMap::with_capacity(0);

    assert!(block_on(m.insert(1, 1)).is_none());

    assert!(block_on(m.contains_key(&1)));
    assert!(!block_on(m.contains_key(&0)));
}

#[test]
fn insert() {
    let m = CHashMap::new();
    assert_eq!(m.len(), 0);
    assert!(block_on(m.insert(1, 2)).is_none());
    assert_eq!(m.len(), 1);
    assert!(block_on(m.insert(2, 4)).is_none());
    assert_eq!(m.len(), 2);
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    block_on(m.with(&2, |r| assert_eq!(*r.unwrap(), 4)));
}

#[test]
fn upsert() {
    let m = CHashMap::new();
    assert_eq!(m.len(), 0);
    block_on(m.upsert(1, || 2, |_| unreachable!()));
    assert_eq!(m.len(), 1);
    block_on(m.upsert(2, || 4, |_| unreachable!()));
    assert_eq!(m.len(), 2);
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    block_on(m.with(&2, |r| assert_eq!(*r.unwrap(), 4)));
}

#[test]
fn upsert_update() {
    let m = CHashMap::new();
    block_on(m.insert(1, 2));
    block_on(m.upsert(1, || unreachable!(), |x| *x += 2));
    block_on(m.insert(2, 3));
    block_on(m.upsert(2, || unreachable!(), |x| *x += 3));
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 4)));
    block_on(m.with(&2, |r| assert_eq!(*r.unwrap(), 6)));
}

#[test]
fn alter_string() {
    let m = CHashMap::new();
    assert_eq!(m.len(), 0);
    block_on(m.alter(1, |_| Some(String::new())));
    assert_eq!(m.len(), 1);
    block_on(m.alter(1, |x| {
        let mut x = x.unwrap();
        x.push('a');
        Some(x)
    }));
    assert_eq!(m.len(), 1);
    block_on(m.with(&1, |r| assert_eq!(&*r.unwrap(), "a")));
}

#[test]
fn clear() {
    let m = CHashMap::new();
    assert!(block_on(m.insert(1, 2)).is_none());
    assert!(block_on(m.insert(2, 4)).is_none());
    assert_eq!(m.len(), 2);

    let om = block_on(m.clear());
    assert_eq!(om.len(), 2);
    block_on(om.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    block_on(om.with(&2, |r| assert_eq!(*r.unwrap(), 4)));

    assert!(m.is_empty());
    assert_eq!(m.len(), 0);

    block_on(m.with(&1, |r| assert_eq!(r, None)));
    block_on(m.with(&2, |r| assert_eq!(r, None)));
}

#[test]
fn clear_with_retain() {
    let m = CHashMap::new();
    assert!(block_on(m.insert(1, 2)).is_none());
    assert!(block_on(m.insert(2, 4)).is_none());
    assert_eq!(m.len(), 2);

    block_on(m.retain(|_, _| false));

    assert!(m.is_empty());
    assert_eq!(m.len(), 0);

    block_on(m.with(&1, |r| assert_eq!(r, None)));
    block_on(m.with(&2, |r| assert_eq!(r, None)));
}

#[test]
fn retain() {
    let m = CHashMap::new();
    block_on(m.insert(1, 8));
    block_on(m.insert(2, 9));
    block_on(m.insert(3, 4));
    block_on(m.insert(4, 7));
    block_on(m.insert(5, 2));
    block_on(m.insert(6, 5));
    block_on(m.insert(7, 2));
    block_on(m.insert(8, 3));

    block_on(m.retain(|key, val| key & 1 == 0 && val & 1 == 1));

    assert_eq!(m.len(), 4);

    for (key, val) in m {
        assert_eq!(key & 1, 0);
        assert_eq!(val & 1, 1);
    }
}

thread_local! { static DROP_VECTOR: RefCell<Vec<isize>> = RefCell::new(Vec::new()) }

#[derive(Hash, PartialEq, Eq)]
struct Dropable {
    k: usize,
}

impl Dropable {
    fn new(k: usize) -> Dropable {
        DROP_VECTOR.with(|slot| {
            slot.borrow_mut()[k] += 1;
        });

        Dropable { k: k }
    }
}

impl Drop for Dropable {
    fn drop(&mut self) {
        DROP_VECTOR.with(|slot| {
            slot.borrow_mut()[self.k] -= 1;
        });
    }
}

impl Clone for Dropable {
    fn clone(&self) -> Dropable {
        Dropable::new(self.k)
    }
}

#[test]
fn drops() {
    DROP_VECTOR.with(|slot| {
        *slot.borrow_mut() = vec![0; 200];
    });

    {
        let m = CHashMap::new();

        DROP_VECTOR.with(|v| {
            for i in 0..200 {
                assert_eq!(v.borrow()[i], 0);
            }
        });

        for i in 0..100 {
            let d1 = Dropable::new(i);
            let d2 = Dropable::new(i + 100);
            block_on(m.insert(d1, d2));
        }

        DROP_VECTOR.with(|v| {
            for i in 0..200 {
                assert_eq!(v.borrow()[i], 1);
            }
        });

        for i in 0..50 {
            let k = Dropable::new(i);
            let v = block_on(m.remove(&k));

            assert!(v.is_some());

            DROP_VECTOR.with(|v| {
                assert_eq!(v.borrow()[i], 1);
                assert_eq!(v.borrow()[i + 100], 1);
            });
        }

        DROP_VECTOR.with(|v| {
            for i in 0..50 {
                assert_eq!(v.borrow()[i], 0);
                assert_eq!(v.borrow()[i + 100], 0);
            }

            for i in 50..100 {
                assert_eq!(v.borrow()[i], 1);
                assert_eq!(v.borrow()[i + 100], 1);
            }
        });
    }

    DROP_VECTOR.with(|v| {
        for i in 0..200 {
            assert_eq!(v.borrow()[i], 0);
        }
    });
}

#[test]
fn move_iter_drops() {
    DROP_VECTOR.with(|v| {
        *v.borrow_mut() = vec![0; 200];
    });

    let hm = {
        let hm = CHashMap::new();

        DROP_VECTOR.with(|v| {
            for i in 0..200 {
                assert_eq!(v.borrow()[i], 0);
            }
        });

        for i in 0..100 {
            let d1 = Dropable::new(i);
            let d2 = Dropable::new(i + 100);
            block_on(hm.insert(d1, d2));
        }

        DROP_VECTOR.with(|v| {
            for i in 0..200 {
                assert_eq!(v.borrow()[i], 1);
            }
        });

        hm
    };

    {
        let mut half = hm.into_iter().take(50);

        DROP_VECTOR.with(|v| {
            for i in 0..200 {
                assert_eq!(v.borrow()[i], 1);
            }
        });

        for _ in half.by_ref() {}

        DROP_VECTOR.with(|v| {
            let nk = (0..100).filter(|&i| v.borrow()[i] == 1).count();

            let nv = (0..100).filter(|&i| v.borrow()[i + 100] == 1).count();

            assert_eq!(nk, 50);
            assert_eq!(nv, 50);
        });
    };

    DROP_VECTOR.with(|v| {
        for i in 0..200 {
            assert_eq!(v.borrow()[i], 0);
        }
    });
}

#[test]
fn empty_pop() {
    let m: CHashMap<isize, bool> = CHashMap::new();
    assert_eq!(block_on(m.remove(&0)), None);
}

#[test]
fn lots_of_insertions() {

    let mut tokio_runtime = Builder::new().threaded_scheduler().build().unwrap();

    tokio_runtime.block_on(async {

        let m : CHashMap<usize, usize> = CHashMap::new();

        // Try this a few times to make sure we never screw up the hashmap's internal state.
        for _ in 0..10 {
            assert!(m.is_empty());

            let mut now = SystemTime::now();
            let mut w_time = 0u128;
            let mut r1_time = 0u128;
            let mut r2_time = 0u128;

            for i in 1..1001 {
                let now = SystemTime::now();
                assert!(m.insert(i, i).await.is_none());
                w_time += now.elapsed().unwrap().as_micros();

                let now = SystemTime::now();
                for j in 1..i + 1 {
                    m.with(&j, |r| assert_eq!(*r.unwrap(), j)).await;
                }
                r1_time += now.elapsed().unwrap().as_micros();

                let now = SystemTime::now();

                for j in i + 1..1001 {
                    assert!(m.with(&j, |v| v.is_none()).await);
                }
                r2_time += now.elapsed().unwrap().as_micros();
            }

            println!("1 : {:?} - R1: {:?} R2: {:?} W: {:?}",
                now.elapsed().unwrap().as_millis(),
                r1_time / 1_000_000u128,
                r2_time / 1_000_000u128,
                w_time / 1_000u128);

            now = SystemTime::now();

            for i in 1001..2001 {
                assert!(!m.contains_key(&i).await);
            }

            println!("2 : {:?}", now.elapsed().unwrap().as_millis());
            now = SystemTime::now();

            // remove forwards
            for i in 1..1001 {
                assert!(m.remove(&i).await.is_some());

                for j in 1..i + 1 {
                    assert!(!m.contains_key(&j).await);
                }

                for j in i + 1..1001 {
                    assert!(m.contains_key(&j).await);
                }
            }

            println!("3 : {:?}", now.elapsed().unwrap().as_millis());
            now = SystemTime::now();

            for i in 1..1001 {
                assert!(!m.contains_key(&i).await);
            }

            println!("4 : {:?}", now.elapsed().unwrap().as_millis());
            now = SystemTime::now();

            for i in 1..1001 {
                assert!(m.insert(i, i).await.is_none());
            }

            println!("5 : {:?}", now.elapsed().unwrap().as_millis());
            now = SystemTime::now();

            // remove backwards
            for i in (1..1001).rev() {
                assert!(m.remove(&i).await.is_some());

                for j in i..1001 {
                    assert!(!m.contains_key(&j).await);
                }

                for j in 1..i {
                    assert!(m.contains_key(&j).await);
                }
            }

            println!("6 : {:?}", now.elapsed().unwrap().as_millis());
            now = SystemTime::now();
        }
    });
}

#[test]
fn find_mut() {
    let m = CHashMap::new();
    assert!(block_on(m.insert(1, 12)).is_none());
    assert!(block_on(m.insert(2, 8)).is_none());
    assert!(block_on(m.insert(5, 14)).is_none());
    let new = 100;
    block_on(m.with_mut(&5, |r| match r {
        None => panic!(),
        Some(x) => *x = new,
    }));
    block_on(m.with(&5, |r| assert_eq!(*r.unwrap(), new)));
}

#[test]
fn insert_overwrite() {
    let m = CHashMap::new();
    assert_eq!(m.len(), 0);
    assert!(block_on(m.insert(1, 2)).is_none());
    assert_eq!(m.len(), 1);
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    assert_eq!(m.len(), 1);
    assert!(!block_on(m.insert(1, 3)).is_none());
    assert_eq!(m.len(), 1);
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 3)));
}

#[test]
fn insert_conflicts() {
    let m = CHashMap::with_capacity(4);
    assert!(block_on(m.insert(1, 2)).is_none());
    assert!(block_on(m.insert(5, 3)).is_none());
    assert!(block_on(m.insert(9, 4)).is_none());
    block_on(m.with(&9, |r| assert_eq!(*r.unwrap(), 4)));
    block_on(m.with(&5, |r| assert_eq!(*r.unwrap(), 3)));
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
}

#[test]
fn conflict_remove() {
    let m = CHashMap::with_capacity(4);
    assert!(block_on(m.insert(1, 2)).is_none());
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    assert!(block_on(m.insert(5, 3)).is_none());
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    block_on(m.with(&5, |r| assert_eq!(*r.unwrap(), 3)));
    assert!(block_on(m.insert(9, 4)).is_none());
    block_on(m.with(&1, |r| assert_eq!(*r.unwrap(), 2)));
    block_on(m.with(&5, |r| assert_eq!(*r.unwrap(), 3)));
    block_on(m.with(&9, |r| assert_eq!(*r.unwrap(), 4)));
    assert!(block_on(m.remove(&1)).is_some());
    block_on(m.with(&9, |r| assert_eq!(*r.unwrap(), 4)));
    block_on(m.with(&5, |r| assert_eq!(*r.unwrap(), 3)));
}

#[test]
fn is_empty() {
    let m = CHashMap::with_capacity(4);
    assert!(block_on(m.insert(1, 2)).is_none());
    assert!(!m.is_empty());
    assert!(block_on(m.remove(&1)).is_some());
    assert!(m.is_empty());
}

#[test]
fn pop() {
    let m = CHashMap::new();
    block_on(m.insert(1, 2));
    assert_eq!(block_on(m.remove(&1)), Some(2));
    assert_eq!(block_on(m.remove(&1)), None);
}

#[test]
fn find() {
    let m = CHashMap::new();
    assert!(block_on(m.with(&1, |r| r.is_none())));
    block_on(m.insert(1, 2));
    block_on(m.with(&1, |r| match r {
        None => panic!(),
        Some(v) => assert_eq!(*v, 2),
    }));
}

#[test]
fn reserve_shrink_to_fit() {
    let m = CHashMap::new();
    block_on(m.insert(0, 0));
    block_on(m.remove(&0));
    assert!(block_on(m.capacity()) >= m.len());
    for i in 0..128 {
        block_on(m.insert(i, i));
    }
    block_on(m.reserve(256));

    let usable_cap = block_on(m.capacity());
    for i in 128..(128 + 256) {
        block_on(m.insert(i, i));
        assert_eq!(block_on(m.capacity()), usable_cap);
    }

    for i in 100..(128 + 256) {
        assert_eq!(block_on(m.remove(&i)), Some(i));
    }
    block_on(m.shrink_to_fit());

    assert_eq!(m.len(), 100);
    assert!(!m.is_empty());
    assert!(block_on(m.capacity()) >= m.len());

    for i in 0..100 {
        assert_eq!(block_on(m.remove(&i)), Some(i));
    }
    block_on(m.shrink_to_fit());
    block_on(m.insert(0, 0));

    assert_eq!(m.len(), 1);
    assert!(block_on(m.capacity()) >= m.len());
    assert_eq!(block_on(m.remove(&0)), Some(0));
}

#[test]
fn from_iter() {
    let xs = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)];

    let map: CHashMap<_, _> = xs.iter().cloned().collect();

    for &(k, v) in &xs {
        block_on(map.with(&k, |r| assert_eq!(*r.unwrap(), v)));
    }
}

#[test]
fn capacity_not_less_than_len() {
    let a = CHashMap::new();
    let mut item = 0;

    for _ in 0..116 {
        block_on(a.insert(item, 0));
        item += 1;
    }

    assert!(block_on(a.capacity()) > a.len());

    let free = block_on(a.capacity()) - a.len();
    for _ in 0..free {
        block_on(a.insert(item, 0));
        item += 1;
    }

    assert_eq!(a.len(), block_on(a.capacity()));

    // Insert at capacity should cause allocation.
    block_on(a.insert(item, 0));
    assert!(block_on(a.capacity()) > a.len());
}

#[test]
fn insert_into_map_full_of_free_buckets() {
    let m = CHashMap::with_capacity(1);
    for i in 0..100 {
        block_on(m.insert(i, 0));
        block_on(m.remove(&i));
    }
}

#[test]
fn lookup_borrowed() {
    let m = CHashMap::with_capacity(1);
    block_on(m.insert("v".to_owned(), "value"));
    block_on(m.with("v", |r| { r.unwrap(); }));
}
