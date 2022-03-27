use crate::engine::KVStoreEngine;
use crate::error::{Error, Result};

pub struct KVPair<K, V>
{
    key: K,
    value: V,
}

impl<K, V> KVPair<K,V> {
    pub fn new(key: K, value: V) -> Self {
        Self{
            key,
            value
        }
    }

    pub fn setvalue(&mut self, value: V) -> Result<()> {
        self.value = value;
        Ok(())
    }
}

pub struct ArrayKVStore<K, V>{
    inner: Vec<KVPair<K, V>>,
}

impl<K: PartialEq,V> ArrayKVStore<K,V> {
    pub fn new() -> Self {
        Self{
            inner: Vec::new()
        }
    }

    pub fn contains(&self, key: &K) -> Option<usize> {
        let pos = self.inner.iter().position(|item| item.key == *key);
        pos
    }
}

impl<K, V> KVStoreEngine<K, V> for ArrayKVStore<K,V>  where V: Clone, K: PartialEq{

    fn get(&self, key: &K) -> Result<V> {
        if let Some(pos) = self.contains(key) {
            Ok(self.inner[pos].value.clone())
        }
        else{
            Err(Error::KeyNotFound)
        }
    }
    fn set(&mut self, key: K, value: V) -> Result<()> {

        if let Some(pos) = self.contains(&key) {
            self.inner[pos].setvalue(value);
        }
        else{
            let kv = KVPair::new(key, value);
            self.inner.push(kv);
        }
        Ok(())
    }

    fn remove(&mut self, key: &K) -> Result<()> {
        if let Some(pos) = self.contains(key){
            self.inner.remove(pos);
            Ok(())
        }
        else{
            Err(Error::KeyNotFound)
        }
    }
}


#[cfg(test)]
mod tests{
    use crate::engine::array::ArrayKVStore;
    use std::collections::HashMap;
    use crate::engine::KVStoreEngine;

    #[test]
    fn test_kv() {
        let mut kvengine:ArrayKVStore<i32, i32> = ArrayKVStore::new();
        kvengine.set(1,2);
        kvengine.remove(&1).unwrap();
        assert_eq!(kvengine.get(&1).unwrap(),2);


    }
    #[test]
    fn test_kv2() {

    }

}
