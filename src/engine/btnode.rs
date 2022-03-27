#[allow(dead_code)]
use std::fmt::Debug;
use bincode::{Serializer, Deserializer};
use serde::{de::DeserializeOwned, Serialize};
use crate::engine::page::{Page, Pager, PagePtr, PAGE_SIZE};
use crate::error::{Error, Result};
use crate::engine::bptree::BPTree;
use std::convert::{TryFrom, TryInto};

const LEAF_NODE_TYPE: u8 = 0;
const INNER_NODE_TYPE: u8 = 1;

const PAGE_PTR_LEN: usize = 8;
const KEYS_LEN: usize = 8;
const VALUES_LEN: usize = 8;
const CHILD_PTRS_LEN: usize = 8;

const PAGE_PTR_OFFSET: usize = 0;
const NODE_TYPE_OFFSET: usize = PAGE_PTR_LEN; //8
const HAS_NEXT_OFFSET: usize = PAGE_PTR_LEN + 1; //9
const NEXT_PAGE_PTR_OFFSET: usize = HAS_NEXT_OFFSET + 1;//10
const KEYS_LEN_OFFSET: usize = NEXT_PAGE_PTR_OFFSET + PAGE_PTR_LEN;//18
const VALUES_LEN_OFFSET: usize = KEYS_LEN_OFFSET + KEYS_LEN;//26
const CHILD_PTRS_LEN_OFFSET: usize =  KEYS_LEN_OFFSET + KEYS_LEN;//26



#[derive(Debug)]
pub struct LeafNode<K, V>
{
    ptr: PagePtr,
    keys: Vec<K>,
    values: Vec<V>,
    next: Option<PagePtr>,
}

impl<K, V> LeafNode<K, V>
    where K: Debug + Clone + Ord + Serialize + DeserializeOwned,
          V: Debug + Clone + Ord + Serialize + DeserializeOwned
{
    pub fn new(page_ptr: PagePtr) -> Self{
        Self{
            ptr: page_ptr,
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        }
    }

    pub fn from(page_ptr: PagePtr, keys: &[K], entries: &[V], next: Option<PagePtr>) -> Self {
        Self{
            ptr: page_ptr,
            keys: keys.to_vec(),
            values: entries.to_vec(),
            next,
        }
    }

    pub fn store_node_to_page(&self, pager: &mut Pager) -> Result<()> {
        let mut bytes = [0u8; PAGE_SIZE];
        let keys_bytes = bincode::serialize(&self.keys)?;
        let values_bytes = bincode::serialize(&self.values)?;
        let keys_bytes_len = keys_bytes.len();
        let values_bytes_len = values_bytes.len() ;

        bytes[PAGE_PTR_OFFSET..PAGE_PTR_OFFSET + PAGE_PTR_LEN].clone_from_slice(&(self.ptr as u64).to_be_bytes());
        bytes[NODE_TYPE_OFFSET] =  LEAF_NODE_TYPE;
        if self.next.is_some(){
            bytes[HAS_NEXT_OFFSET] = 1;
            bytes[NEXT_PAGE_PTR_OFFSET..NEXT_PAGE_PTR_OFFSET + PAGE_PTR_LEN].clone_from_slice(&(self.next.unwrap() as u64).to_be_bytes());
        }
        bytes[KEYS_LEN_OFFSET..KEYS_LEN_OFFSET + KEYS_LEN].clone_from_slice(&(keys_bytes_len as u64).to_be_bytes());
        bytes[VALUES_LEN_OFFSET..VALUES_LEN_OFFSET + VALUES_LEN].clone_from_slice(&(values_bytes_len as u64).to_be_bytes());
        if keys_bytes_len > 0 {
            bytes[VALUES_LEN_OFFSET + VALUES_LEN..VALUES_LEN_OFFSET + VALUES_LEN + keys_bytes_len]
                .clone_from_slice(keys_bytes.as_slice());
        }
        if values_bytes_len > 0 {
            bytes[VALUES_LEN_OFFSET + VALUES_LEN + keys_bytes_len..
                VALUES_LEN_OFFSET + VALUES_LEN + keys_bytes_len + values_bytes_len]
                .clone_from_slice(values_bytes.as_slice());
        }

        let page = Page::from_bytes(bytes);
        match pager.insert_page(self.ptr, &page) {
            Ok(()) => {Ok(())},
            Err(Error::PageNotFound) => {pager.append_page(&page)}
            Err(e) => {Err(e)}
        }
    }

    pub fn load_node_from_page(mut self, page: Page) -> Result<Self> {
        let bytes = page.get_page_data();
        self.ptr = u64::from_be_bytes(bytes[PAGE_PTR_OFFSET..PAGE_PTR_OFFSET + PAGE_PTR_LEN].try_into().unwrap());
        if bytes[HAS_NEXT_OFFSET] == 0 {
            self.next = Option::None;
        }
        else{
            self.next = Some(u64::from_be_bytes(bytes[NEXT_PAGE_PTR_OFFSET..NEXT_PAGE_PTR_OFFSET + PAGE_PTR_LEN].try_into().unwrap()));
        }
        let keys_bytes_len = usize::from_be_bytes(bytes[KEYS_LEN_OFFSET..KEYS_LEN_OFFSET + KEYS_LEN].try_into().unwrap());
        let values_bytes_len = usize::from_be_bytes(bytes[VALUES_LEN_OFFSET..VALUES_LEN_OFFSET + VALUES_LEN].try_into().unwrap());
        if keys_bytes_len > 0 {
            self.keys = bincode::deserialize(&bytes[VALUES_LEN_OFFSET + VALUES_LEN..VALUES_LEN_OFFSET + VALUES_LEN + keys_bytes_len])?;
        }
        if values_bytes_len > 0 {
            self.values = bincode::deserialize(&bytes[VALUES_LEN_OFFSET + VALUES_LEN + keys_bytes_len..
                VALUES_LEN_OFFSET + VALUES_LEN + keys_bytes_len + values_bytes_len])?;
        }
        Ok(self)
    }

    pub fn get(&self, key: &K) -> Option<V>{
        match self.keys.binary_search(key) {
            Ok(i) => {Some(self.values[i].clone())},
            Err(_) => Option::None,
        }
    }

    fn insert(&mut self, i: usize, key: K, value: V) {
        self.keys.insert(i, key);
        self.values.insert(i, value);
    }

    pub fn set(&mut self, key: K, value: V, bptree: &mut BPTree<K, V>) -> Result<Option<(K, PagePtr)>> {
        match self.keys.binary_search(&key) {
            Ok(i) => {
                self.values[i] = value;
                Ok(Option::None)
            }
            Err(i) => match self.is_full(bptree.max_key_count()){
                true => {
                    let (split_key, mut new_leaf) = self.split(bptree.next_page_ptr(), bptree.split_at())?;
                    let new_leaf_ptr = new_leaf.ptr;
                    match i < bptree.split_at() {
                        true => self.insert(i, key, value),
                        false => new_leaf.insert(i - bptree.split_at(), key, value),
                    }
                    self.store_node_to_page(bptree.get_pager())?;
                    new_leaf.store_node_to_page(bptree.get_pager())?;
                    Ok(Some((split_key, new_leaf_ptr)))
                },
                false => {
                    self.insert(i, key, value);
                    self.store_node_to_page(bptree.get_pager())?;
                    Ok(None)
                }
            }
        }
    }

    pub fn remove(
        mut self,
        key: &K,
        parent: Option<&mut InnerNode<K>>,
        path_info: Option<&ChildNodeInfo>,
        bptree: &mut BPTree<K, V>,
    ) -> Result<(Option<V>, Option<PagePtr>)> {
        match self.keys.binary_search(key) {
            Err(_) => Ok((None, None)),
            Ok(i) => {
                self.keys.remove(i);
                let original_value = self.values.remove(i);
                let mut delete_page = None;
                if self.keys.len() < bptree.split_at() && parent.is_some() {
                    let parent = parent.unwrap();
                    let path_info = path_info.unwrap();
                    let mut done = false;
                    if path_info.lsibling.is_some() {
                        let mut node = LeafNode::new(path_info.lsibling.unwrap())
                            .load_node_from_page(bptree.get_pager().load_page(path_info.lsibling.unwrap())?)?;
                        if node.keys.len() > bptree.split_at() {
                            let k: K = node.keys.pop().unwrap();
                            let v = node.values.pop().unwrap();
                            self.keys.insert(0, k.clone());
                            self.values.insert(0, v);
                            parent.keys[path_info.rparent.unwrap()] = k;
                            node.store_node_to_page(bptree.get_pager())?;
                            done = true;
                        }
                    }
                    if !done && path_info.rsibling.is_some(){
                        let mut node = LeafNode::new(path_info.rsibling.unwrap())
                            .load_node_from_page(bptree.get_pager().load_page(path_info.rsibling.unwrap())?)?;
                        if node.keys.len() > bptree.split_at() {
                            let k = node.keys.remove(0);
                            let v = node.values.remove(0);
                            self.keys.push(k);
                            self.values.push(v);
                            parent.keys[path_info.lparent.unwrap()] = node.keys[0].clone();
                            node.store_node_to_page(bptree.get_pager())?;
                            done = true;
                        }
                    }
                    if !done {
                        if path_info.lsibling.is_some() {
                            let mut node = LeafNode::new(path_info.lsibling.unwrap())
                                .load_node_from_page(bptree.get_pager().load_page(path_info.lsibling.unwrap())?)?;
                            node.keys.extend(self.keys);
                            node.values.extend(self.values);
                            node.next = self.next;
                            delete_page = Some(self.ptr);
                            bptree.delete_page(self.ptr);
                            self = node;
                        }
                        else if path_info.rsibling.is_some() && path_info.rsibling == self.next{
                            let mut node = LeafNode::new(path_info.rsibling.unwrap())
                                .load_node_from_page(bptree.get_pager().load_page(path_info.rsibling.unwrap())?)?;
                            self.keys.extend(node.keys);
                            self.values.extend(node.values);
                            self.next = node.next;
                            delete_page = Some(node.ptr);
                            bptree.delete_page(node.ptr);
                        }
                    }

                }
                self.store_node_to_page(bptree.get_pager())?;
                Ok((Some(original_value), delete_page))
            }
        }
    }
    pub fn is_full(&self, max_key_cout: u64) -> bool {
        self.keys.len() >= max_key_cout as usize
    }

    pub fn split(&mut self, next_ptr: PagePtr, split_at: usize) -> Result<(K, Self)> {
        let split_key = self.keys[split_at].clone();
        let mut node = Self::from(next_ptr, &self.keys[split_at..], &self.values[split_at..], self.next);
        self.next = Some(next_ptr);
        self.keys.drain(split_at..);
        self.values.drain(split_at..);
        Ok((split_key, node))
    }

}

#[derive(Debug)]
struct ChildNodeInfo {
    page_nr: PagePtr,
    lparent: Option<usize>, // LeftSubtree(keys[lparent]) == page_nr
    rparent: Option<usize>, // RightSubtree(keys[rparent]) == page_nr
    lsibling: Option<PagePtr>,
    rsibling: Option<PagePtr>,
}


#[derive(Debug)]
pub struct InnerNode<K>
{
    ptr: PagePtr,
    keys: Vec<K>,
    childptrs: Vec<PagePtr>,
}

impl<K> InnerNode<K>
    where K: Debug + Clone + Ord + Serialize + DeserializeOwned
{
    pub fn new(page_ptr: PagePtr) -> Self {
        Self{
            ptr: page_ptr,
            keys: Vec::new(),
            childptrs: Vec::new(),
        }
    }

    pub fn from(page_ptr: PagePtr, keys: &[K], entries: &[PagePtr]) -> Self {
        Self{
            ptr: page_ptr,
            keys: keys.to_vec(),
            childptrs: entries.to_vec(),
        }
    }
    pub fn store_node_to_page(&self, pager: &mut Pager) -> Result<()> {
        let mut bytes = [0u8; PAGE_SIZE];
        let keys_bytes = bincode::serialize(&self.keys)?;
        let childptrs_bytes = bincode::serialize(&self.childptrs)?;
        let keys_bytes_len = keys_bytes.len();
        let childptrs_bytes_len = childptrs_bytes.len() ;

        bytes[PAGE_PTR_OFFSET..PAGE_PTR_OFFSET + PAGE_PTR_LEN].clone_from_slice(&(self.ptr as u64).to_be_bytes());
        bytes[NODE_TYPE_OFFSET] =  INNER_NODE_TYPE;
        bytes[KEYS_LEN_OFFSET..KEYS_LEN_OFFSET + KEYS_LEN].clone_from_slice(&(keys_bytes_len as u64).to_be_bytes());
        bytes[CHILD_PTRS_LEN_OFFSET..CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN].clone_from_slice(&(childptrs_bytes_len as u64).to_be_bytes());
        if keys_bytes_len > 0 {
            bytes[CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN..CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN + keys_bytes_len]
                .clone_from_slice(keys_bytes.as_slice());
        }
        if childptrs_bytes_len > 0 {
            bytes[CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN + keys_bytes_len..
                CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN + keys_bytes_len + childptrs_bytes_len]
                .clone_from_slice(childptrs_bytes.as_slice());
        }

        let page = Page::from_bytes(bytes);
        match pager.insert_page(self.ptr, &page) {
            Ok(()) => {Ok(())},
            Err(Error::PageNotFound) => {pager.append_page(&page)}
            Err(e) => {Err(e)}
        }
    }

    pub fn load_node_from_page(mut self, page: Page) -> Result<Self> {
        let bytes = page.get_page_data();
        self.ptr = u64::from_be_bytes(bytes[PAGE_PTR_OFFSET..PAGE_PTR_OFFSET + PAGE_PTR_LEN].try_into().unwrap());
        let keys_bytes_len = usize::from_be_bytes(bytes[KEYS_LEN_OFFSET..KEYS_LEN_OFFSET + KEYS_LEN].try_into().unwrap());
        let childptrs_bytes_len = usize::from_be_bytes(bytes[CHILD_PTRS_LEN_OFFSET..CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN].try_into().unwrap());
        if keys_bytes_len > 0 {
            self.keys = bincode::deserialize(&bytes[CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN..CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN + keys_bytes_len])?;
        }
        if childptrs_bytes_len > 0 {
            self.childptrs = bincode::deserialize(&bytes[CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN + keys_bytes_len..
                CHILD_PTRS_LEN_OFFSET + CHILD_PTRS_LEN + keys_bytes_len + childptrs_bytes_len])?;
        }
        Ok(self)
    }

    pub fn get(&self, key: &K) -> PagePtr {
        match self.keys.binary_search(key) {
            Ok(i) => self.childptrs[i+1],
            Err(i) => self.childptrs[i]
        }
    }

    pub fn set<V>(&mut self, key: K, value: V, bptree: &mut BPTree<K, V>) -> Result<(Option<(K, PagePtr)>)>
    where
        V: Debug + Clone + Ord  +  Serialize + DeserializeOwned,
    {
        let child_ptr = self.get(&key);
        let return_value = match Node::load_node(child_ptr, bptree.get_pager())?{
            Node::Leaf(mut leaf_node) => {leaf_node.set(key, value, bptree)?},
            Node::Inner(mut inner_node) =>{inner_node.set(key, value,bptree)?}
        };
        match return_value {
            None => Ok(None),
            Some((split_key, split_page_ptr)) => match self.keys.binary_search(&split_key) {
                Ok(_) => panic!("Programming error: key should not be present!"),
                Err(i) => match self.is_full(bptree.max_key_count()) {
                    true => {
                        let (new_split_key, mut new_split_node) = self.split(bptree.next_page_ptr(), bptree.split_at())?;
                        let new_page_ptr = new_split_node.ptr;
                        match i < bptree.split_at() {
                            true => self.insert(i, split_key, split_page_ptr),
                            false => new_split_node.insert(i - bptree.split_at() - 1, split_key, split_page_ptr),
                        }
                        self.store_node_to_page(bptree.get_pager())?;
                        new_split_node.store_node_to_page(bptree.get_pager())?;
                        Ok(Some((new_split_key, new_page_ptr)))
                    }
                    false => {
                        self.insert(i, split_key, split_page_ptr);
                        self.store_node_to_page(bptree.get_pager())?;
                        Ok(None)
                    }
                }
            }
        }
    }

    fn get_child_node_info(&self, key: &K) -> ChildNodeInfo {
        match self.keys.binary_search(key) {
            Ok(i) => {
                // exact match -> right subtree
                ChildNodeInfo {
                    page_nr: self.childptrs[i + 1],
                    lparent: if i < self.keys.len() - 1 { Some(i + 1) } else { None },
                    rparent: Some(i),
                    lsibling: Some(self.childptrs[i]),
                    rsibling: if i < self.childptrs.len() - 2 { Some(self.childptrs[i + 2]) } else { None },
                }
            }
            Err(i) => {
                // not found: keys(i) > key -> left subtree
                ChildNodeInfo {
                    page_nr: self.childptrs[i],
                    lparent: Some(i),
                    rparent: if i > 0 { Some(i - 1) } else { None },
                    lsibling: if i > 0 { Some(self.childptrs[i - 1]) } else { None },
                    rsibling: if i < self.childptrs.len() - 1 { Some(self.childptrs[i + 1]) } else { None },
                }
            }
        }
    }

    pub fn remove<V>(
        mut self,
        key: &K,
        parent: Option<&mut InnerNode<K>>,
        path_info: Option<&ChildNodeInfo>,
        bptree: &mut BPTree<K, V>
    ) -> Result<(Option<V>, Option<PagePtr>)>
    where
        V: Debug + Clone + Ord  +  Serialize + DeserializeOwned,
    {
        let child_info = self.get_child_node_info(&key);
        let (original_value, deleted_page) = match Node::load_node(child_info.page_nr, bptree.get_pager())? {
            Node::Leaf(mut leaf_node) => leaf_node.remove(key, Some(&mut self), Some(&child_info), bptree)?,
            Node::Inner(mut inner_node) => inner_node.remove(key,Some(&mut self), Some(&child_info), bptree)?,
        };
        let result = match deleted_page {
            None => Ok((original_value, None)),
            Some(page_nr) => {
                let deleted_page = self.remove_page(page_nr, parent, path_info, bptree)?;
                Ok((original_value, deleted_page))
            }
        };
        self.store_node_to_page(bptree.get_pager())?;
        result
    }

    pub fn remove_page<V>(
        &mut self,
        page_ptr: PagePtr,
        parent: Option<&mut InnerNode<K>>,
        path_info: Option<&ChildNodeInfo>,
        bptree: &mut BPTree<K, V>
    ) -> Result<Option<PagePtr>>
    where
        V: Debug + Clone + Ord  +  Serialize + DeserializeOwned,
    {
        match self.childptrs.binary_search(&page_ptr) {
            Err(_) => panic!("Programming error: deleted page should be present!"),
            Ok(i) => {
                self.keys.remove(i-1);
                self.childptrs.remove(i);
                let deleted_page_ptr = match parent{
                    None => {
                        if self.keys.len() == 0 {
                            let new_root_page_ptr = self.childptrs[0];
                            bptree.set_root(Some(new_root_page_ptr));
                            bptree.delete_page(self.ptr);
                            Some(self.ptr)
                        }
                        else{
                            None
                        }
                    }
                    Some(parent) => {
                        let mut deleted_page = None;
                        if self.keys.len() < bptree.split_at() {
                            let mut done = false;
                            let path_info = path_info.unwrap();
                            if path_info.lsibling.is_some() {
                                let mut node = InnerNode::new(path_info.lsibling.unwrap())
                                    .load_node_from_page(bptree.get_pager().load_page(path_info.lsibling.unwrap())?)?;
                                if node.keys.len() > bptree.split_at() {
                                    let k: K = node.keys.pop().unwrap();
                                    let v = node.childptrs.pop().unwrap();
                                    self.keys.insert(0, k.clone());
                                    self.childptrs.insert(0, v);
                                    parent.keys[path_info.rparent.unwrap()] = k;
                                    node.store_node_to_page(bptree.get_pager())?;
                                    done = true;
                                }
                            }
                            if !done && path_info.rsibling.is_some(){
                                let mut node = InnerNode::new(path_info.rsibling.unwrap())
                                    .load_node_from_page(bptree.get_pager().load_page(path_info.rsibling.unwrap())?)?;
                                if node.keys.len() > bptree.split_at() {
                                    let k = node.keys.remove(0);
                                    let v = node.childptrs.remove(0);
                                    self.keys.push(k);
                                    self.childptrs.push(v);
                                    parent.keys[path_info.lparent.unwrap()] = node.keys[0].clone();
                                    node.store_node_to_page(bptree.get_pager())?;
                                    done = true;
                                }
                            }
                            if !done {
                                if path_info.lsibling.is_some() {
                                    let mut node = InnerNode::new(path_info.lsibling.unwrap())
                                        .load_node_from_page(bptree.get_pager().load_page(path_info.lsibling.unwrap())?)?;
                                    node.keys.push(parent.keys[path_info.rparent.unwrap()].clone());
                                    node.keys.extend(self.keys.iter().map(|k| k.clone()));
                                    node.childptrs.extend(&self.childptrs);
                                    deleted_page = Some(self.ptr);
                                    node.store_node_to_page(bptree.get_pager())?;
                                }
                                else if path_info.rsibling.is_some(){
                                    let node = InnerNode::new(path_info.rsibling.unwrap())
                                        .load_node_from_page(bptree.get_pager().load_page(path_info.rsibling.unwrap())?)?;
                                    self.keys.push(parent.keys[path_info.lparent.unwrap()].clone());
                                    self.keys.extend(node.keys);
                                    self.childptrs.extend(node.childptrs);
                                    deleted_page = Some(node.ptr);
                                    self.store_node_to_page(bptree.get_pager())?;
                                }
                            }
                        }
                        deleted_page
                    }
                };
                Ok(deleted_page_ptr)
            }
        }
    }

    fn is_full(&self, max_key_cout: u64) -> bool {
        self.keys.len() >= max_key_cout as usize
    }

    fn split(&mut self, next_ptr: PagePtr, split_at: usize) -> Result<(K, Self)> {
        let split_key = self.keys[split_at].clone();
        let mut node = Self::from(next_ptr, &self.keys[split_at+1..], &self.childptrs[split_at+1..]);
        self.keys.drain(split_at..);
        self.childptrs.drain(split_at+1..);
        Ok((split_key, node))
    }

    fn insert(&mut self, i: usize, key: K, value: PagePtr) {
        self.keys.insert(i, key);
        self.childptrs.insert(i + 1, value);
    }

}

pub enum Node<K, V> {
    Leaf(LeafNode<K, V>),
    Inner(InnerNode<K>),
}

impl<K, V> Node<K, V>
    where K: Debug + Clone + Ord + Serialize + DeserializeOwned,
          V: Debug + Clone + Ord + Serialize + DeserializeOwned
{
    pub fn store_node(self, pager: &mut Pager) -> Result<()>{
        match self {
            Self::Leaf(leaf_node) => { leaf_node.store_node_to_page(pager)?; },
            Self::Inner(inner_node) => { inner_node.store_node_to_page(pager)?; }
        }
        Ok(())
    }

    pub fn load_node(page_ptr: PagePtr, pager: &mut Pager) ->Result<Self> {
        let page = pager.load_page(page_ptr)?;
        match page.get_page_byte(NODE_TYPE_OFFSET) {
            LEAF_NODE_TYPE => { Ok(Node::Leaf(LeafNode::new(page_ptr).load_node_from_page(page)?))},
            INNER_NODE_TYPE => {Ok(Node::Inner(InnerNode::new(page_ptr).load_node_from_page(page)?))},
            _ =>{Err(Error::UnkonwNodeType)}
        }
    }

    pub fn get(self, key: &K, pager: &mut Pager) -> Result<Option<V>> {
        match self {
            Self::Leaf(leaf_node) =>{
                Ok(leaf_node.get(key))
            }
            Self::Inner(inner_node) => {
                let mut child_ptr = inner_node.get(key);
                loop {
                    match Self::load_node(child_ptr, pager)? {
                        Self::Leaf(leaf_node) => { return Ok(leaf_node.get(key)) },
                        Self::Inner(inner_node) => { child_ptr = inner_node.get(key);}
                    }
                }
            }
        }
    }

    pub fn set(self, key: K, value: V, bptree: &mut BPTree<K, V>) -> Result<Option<(K,PagePtr)>> {
        match self {
            Self::Leaf(mut leaf_node) => leaf_node.set(key, value, bptree),
            Self::Inner(mut inner_node) => inner_node.set(key, value, bptree),
        }
    }

    pub fn remove(self, key: &K, bptree: &mut BPTree<K, V>) -> Result<(Option<V>, Option<PagePtr>)> {
        match self {
            Self::Leaf(mut leaf_node) => leaf_node.remove(key, None, None, bptree),
            Self::Inner(mut inner_node) => inner_node.remove(key, None, None, bptree),
        }
    }

    pub fn new_leaf(ptr: PagePtr, keys: &[K], entries: &[V], next: Option<PagePtr>) -> Self{
        Self::Leaf(LeafNode::from(ptr, keys, entries, next))
    }

    pub fn new_inner(ptr: PagePtr, keys: &[K], entries: &[PagePtr]) -> Self {
        Self::Inner(InnerNode::from(ptr, keys, entries))
    }
}


#[cfg(test)]
mod test{
    use std::path::Path;
    use super::*;
    #[test]
    fn test_node() -> Result<()> {
        let path = Path::new("data\\t4.txt");
        let mut bptree: BPTree<u128, u128> = BPTree::new(path, Some(5))?;
        for i in 1..=60 {
            bptree.set(i, i*10);
        }
        for i in 1..=60{
            println!("{}", bptree.get(i)?);
            assert_eq!(i*10, bptree.get(i)?);
        }
        for i in 1..=14 {
            let key = i*3;
            bptree.remove(&key)?;
        }
        let p1 = 15;
        let p2 = 17;
        for p in 0..26{
            let n:Node<u128, u128> = Node::load_node(p,bptree.get_pager())?;
            match n{
                Node::Leaf(leaf) => println!("{:?}", leaf),
                Node::Inner(inner) => println!("{:?}", inner),
            }
        }
        bptree.print_deleted();
        for i in 1..=60{
            match bptree.get(i){
                Ok(j) => println!("{}", j),
                Err(e) => {println!("{} is removed", i)}
            }

        }
        Ok(())
    }

}

