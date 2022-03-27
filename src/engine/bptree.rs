#[allow(dead_code)]
use std::borrow::BorrowMut;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use crate::engine::page::{Pager, PagePtr, split_at, max_key_count};
use crate::error::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::mem;
use crate::engine::btnode::{Node, InnerNode, LeafNode};


pub struct BPTree<K,V> {
    root_ptr: Option<PagePtr>,
    pager: Pager,
    page_count: u64,
    key_size: u64,
    value_size: u64,
    key_type: PhantomData<K>,
    value_type: PhantomData<V>,
    max_key_count: u64,
    split_at: usize,
    emtpy_pages: Vec<PagePtr>,
}

impl<K, V> BPTree<K,V>
    where  K: Debug + Clone + Ord + Serialize + DeserializeOwned,
           V: Debug + Clone + Ord + Serialize + DeserializeOwned,
{
    pub fn new<P: AsRef<Path>>(path: P, override_max_key_count: Option<u64>) -> Result<Self>{
        let pager = Pager::open(path)?;
        let key_size = mem::size_of::<K>() as u64;
        let value_size = mem::size_of::<V>() as u64;
        let max_key_count = match override_max_key_count {
            None => max_key_count(key_size, value_size),
            Some(n) => n,
        };
        let split_at = split_at(max_key_count);
        Ok(Self{
            root_ptr: None,
            pager: pager,
            page_count: 0,
            key_size,
            value_size,
            key_type: PhantomData,
            value_type: PhantomData,
            max_key_count,
            split_at,
            emtpy_pages: vec![],
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        todo!()
    }

    pub fn set(&mut self, key: K, value: V) -> Result<()> {
        let mut root_node;
        if self.root_ptr.is_none() {
            root_node = self.create_root_node();
        }
        else{
            root_node = Node::load_node(self.root_ptr.unwrap(), self.get_pager())?;
        }
        if let Some((split_key, new_page_ptr)) = root_node.set(key,value, self)? {
            self.create_new_root(split_key, new_page_ptr)?;
        }
        Ok(())
    }

    pub fn get(&mut self, key: K) -> Result<V> {
        if self.root_ptr.is_none() {
            Err(Error::RootPageIsNull)
        }
        else{
            let root_node: Node<K,V> = Node::load_node(self.root_ptr.unwrap(), self.get_pager())?;
            let  res = root_node.get(&key, self.get_pager())?;
            if res.is_some(){
                Ok(res.unwrap())
            }
            else{
                Err(Error::KeyNotFound)
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Result<()> {
        if self.root_ptr.is_none() {
            Err(Error::RootPageIsNull)
        }
        else{
            let root_node: Node<K,V> = Node::load_node(self.root_ptr.unwrap(), self.get_pager())?;
            root_node.remove(key, self)?;
            Ok(())
        }
    }

    pub fn max_key_count(&self) -> u64 {
        self.max_key_count
    }

    pub fn split_at(&self) -> usize {
        self.split_at
    }

    pub fn next_page_ptr(&mut self) -> PagePtr {
        let next_ptr = self.page_count;
        self.page_count += 1;
        next_ptr
    }

    fn create_root_node(&mut self) -> Node<K,V> {
        self.root_ptr = Some(self.next_page_ptr());
        Node::new_leaf(self.root_ptr.unwrap(), &[], &[], None)
    }

    fn create_new_root(&mut self, key: K, new_page_ptr: PagePtr) -> Result<()> {
        let old_root_ptr = self.root_ptr.unwrap();
        self.root_ptr = Some(self.next_page_ptr());
        let mut new_root: Node<K,V> = Node::new_inner(self.root_ptr.unwrap(), &[key], &[old_root_ptr, new_page_ptr]);
        new_root.store_node(self.get_pager())?;
        Ok(())
    }

    fn load_root(&mut self) -> Result<Node<K, V>> {
        match self.root_ptr {
            None => Err(Error::RootPageIsNull),
            Some(ptr) => Node::load_node(ptr, self.get_pager())
        }
    }

    pub fn get_pager(&mut self) -> &mut Pager {
        self.pager.borrow_mut()
    }

    pub fn set_root(&mut self, new_root_ptr: Option<PagePtr>) {
        self.root_ptr = new_root_ptr;
    }

    pub fn delete_page(&mut self, ptr: PagePtr){
        self.emtpy_pages.push(ptr);
    }

    pub fn print_deleted(&self) {
        println!("{:?}", self.emtpy_pages);
    }
}