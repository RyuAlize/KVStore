#[allow(dead_code)]

use std::path::{PathBuf, Path};
use crate::error::{Result, Error};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};

pub type PagePtr = u64;
pub const PAGE_SIZE: usize = 4096;

pub fn max_key_count(size_key: u64, size_value: u64) -> u64 {
    (PAGE_SIZE as u64 - size_value - 34) / (size_key + size_value)
}

pub fn split_at(max_key_count: u64) -> usize {
    ((max_key_count / 2) + (max_key_count % 2)) as usize
}

pub struct Page{
    data: Box<[u8; PAGE_SIZE]>
}

impl Page{
    pub fn new() -> Self{
        Self{
            data: Box::new([0u8; PAGE_SIZE])
        }
    }

    pub fn from_bytes(bytes: [u8; PAGE_SIZE]) -> Self {
        Self{
            data: Box::new(bytes),
        }
    }

    pub fn write_bytes_at_offset(&mut self, offset: usize, value: &[u8]) -> Result<()>{
        let end = offset+value.len();
        if end > PAGE_SIZE {
            Err(Error::PageSizeNotEnough)
        }
        else{
            for i in offset..end {
                self.data[offset..end].clone_from_slice(value);
            }
            Ok(())
        }
    }

    pub fn get_bytes_from_offset(&self, offset: usize, size: usize) -> Result<&[u8]> {
        let end = offset + size;
        if end > PAGE_SIZE {
            Err(Error::PageSizeNotEnough)
        }
        else{
            let bytes = &self.data[offset..end];
            Ok(bytes)
        }
    }

    pub fn get_page_data(&self) -> [u8; PAGE_SIZE] {
        *self.data
    }

    pub fn get_page_byte(&self, pos: usize) -> u8 {
        self.data[pos]
    }
}


pub struct Pager {
    fd: File,
}

impl Pager{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>{
        let fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self{fd})
    }

    pub fn load_page(&mut self, page_ptr: PagePtr) -> Result<Page> {
        let offset = page_ptr * PAGE_SIZE as u64;
        let file_len = self.fd.seek(SeekFrom::End(0))?;
        if file_len < offset as u64 {
            Err(Error::PageNotFound)
        }
        else{
            let mut bytes = [0u8; PAGE_SIZE];
            self.fd.seek(SeekFrom::Start(offset as u64))?;
            self.fd.read_exact(&mut bytes)?;
            let page = Page::from_bytes(bytes);
            Ok(page)
        }

    }

    pub fn insert_page(&mut self, page_ptr: PagePtr, page: &Page) -> Result<()>{
        let offset = page_ptr * PAGE_SIZE as u64;
        let file_len = self.fd.seek(SeekFrom::End(0))?;
        if file_len < offset as u64 {
            Err(Error::PageNotFound)
        }
        else{
            self.fd.seek(SeekFrom::Start(offset as u64))?;
            let bytes = page.get_page_data();
            self.fd.write_all(&bytes)?;
            Ok(())
        }
    }

    pub fn append_page(&mut self, page: &Page) -> Result<()> {
        let offset = self.fd.seek(SeekFrom::End(0))?;
        self.fd.seek(SeekFrom::Start(offset as u64))?;
        let bytes = page.get_page_data();
        self.fd.write_all(&bytes)?;
        Ok(())
    }
}




