mod array;
mod bptree;
mod btnode;
mod page;

use crate::error::Result;

pub trait KVStoreEngine<K,V> {
    fn get(&self, key: &K) -> Result<V>;
    fn set(&mut self, key: K, value: V) -> Result<()>;
    fn remove(&mut self, key: &K) -> Result<()>;
}

