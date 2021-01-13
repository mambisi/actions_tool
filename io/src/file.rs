use std::io::{BufWriter, Write, BufReader, Read, Seek, SeekFrom, Error};
use std::fs::{File, Permissions, OpenOptions};
use std::path::{Path};
use bytes::{BytesMut, Buf, BufMut, Bytes};
use std::string::FromUtf8Error;
use anyhow::Result;
use anyhow::anyhow;
use bincode::ErrorKind;
use std::fmt::{Display, Formatter};
use crate::channel::{Block, ContextAction};

const HEADER_LEN: usize = 12;

#[derive(Clone, Copy, Debug)]
pub struct ActionsFileHeader {
    pub block_height: u32,
    pub actions_count: u32,
    pub block_count: u32,
}

impl Display for ActionsFileHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut formatter: String = String::new();
        formatter.push_str( &format!("{:<24}{}\n", "Block Height:",self.block_height));
        formatter.push_str( &format!("{:<24}{}\n", "Block Count:",self.block_count));
        formatter.push_str( &format!("{:<24}{}", "Actions Count:",self.actions_count));
        writeln!(f, "{}", formatter)
    }
}


impl From<[u8; HEADER_LEN]> for ActionsFileHeader {
    fn from(v: [u8; 12]) -> Self {
        let mut bytes = BytesMut::with_capacity(v.len());
        bytes.put_slice(&v);
        let block_height = bytes.get_u32();
        let actions_count = bytes.get_u32();
        let block_count = bytes.get_u32();

        ActionsFileHeader {
            block_height,
            actions_count,
            block_count,
        }
    }
}

impl ActionsFileHeader {
    fn to_vec(&self) -> Vec<u8> {
        let mut bytes = BytesMut::with_capacity(HEADER_LEN);
        bytes.put_u32(self.block_height);
        bytes.put_u32(self.actions_count);
        bytes.put_u32(self.block_count);
        bytes.to_vec()
    }
    fn new() -> Self {
        ActionsFileHeader {
            block_height: 0,
            actions_count: 0,
            block_count: 0,
        }
    }
}
/// # ActionFileReader
/// Reads actions binary file in `path`
/// ## Examples
/// ```
/// use io::ActionsFileReader;
///
/// let reader = ActionsFileReader::new("./actions.bin").unwrap();
/// println!("{}", reader.header());
/// ```

pub struct ActionsFileReader {
    header: ActionsFileHeader,
    cursor: u64,
    reader: BufReader<File>,
}


impl ActionsFileReader {

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = OpenOptions::new().write(false).create(false).read(true).open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(0));
        let mut h = [0_u8; HEADER_LEN];
        reader.read_exact(&mut h);
        let header = ActionsFileHeader::from(h);
        Ok(ActionsFileReader {
            reader,
            header,
            cursor: HEADER_LEN as u64,
        })
    }

    /// Prints header `ActionsFileHeader`
    pub fn header(&self) -> ActionsFileHeader {
        self.header
    }

    pub fn fetch_header(&mut self) -> ActionsFileHeader {
        self.reader.seek(SeekFrom::Start(0));
        let mut h = [0_u8; HEADER_LEN];
        self.reader.read_exact(&mut h);
        self.header = ActionsFileHeader::from(h);
        self.header()
    }
}

impl Iterator for ActionsFileReader {
    type Item = (Block, Vec<ContextAction>);

    /// Return a tuple of a block and list action in the block
    fn next(&mut self) -> Option<Self::Item> {
        self.cursor = match self.reader.seek(SeekFrom::Start(self.cursor)) {
            Ok(c) => {
                c
            }
            Err(_) => {
                return None;
            }
        };
        let mut h = [0_u8; 4];
        self.reader.read_exact(&mut h);
        let content_len = u32::from_be_bytes(h);
        if content_len <= 0 {
            return None;
        }
        let mut b = BytesMut::with_capacity(content_len as usize);
        unsafe { b.set_len(content_len as usize) }
        self.reader.read_exact(&mut b);
        let item = match bincode::deserialize::<(Block, Vec<ContextAction>)>(&b) {
            Ok(item) => {
                item
            }
            Err(_) => {
                return None;
            }
        };
        self.cursor += h.len() as u64 + content_len as u64;
        Some(item)
    }
}

/// # ActionFileWriter
///
/// writes block and list actions to file in `path`
pub struct ActionsFileWriter {
    header: ActionsFileHeader,
    file: File,
}


impl ActionsFileWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let mut file = OpenOptions::new().write(true).create(true).read(true).open(path)?;
        let mut reader = BufReader::new(file.try_clone()?);
        reader.seek(SeekFrom::Start(0));
        let mut h = [0_u8; HEADER_LEN];
        reader.read_exact(&mut h);
        let header = ActionsFileHeader::from(h);
        Ok(ActionsFileWriter {
            file,
            header,
        })
    }

    pub fn header(&self) -> ActionsFileHeader {
        self.header
    }
}


unsafe impl Send for ActionsFileWriter {}

unsafe impl Sync for ActionsFileWriter {}


impl ActionsFileWriter {
    pub fn update(&mut self, block: Block, actions: Vec<ContextAction>) -> Result<u32> {
        let block_level = block.block_level;
        let actions_count = actions.len() as u32;

        self._fetch_header();

        if block.block_level <= self.header.block_height && self.header.block_count > 0 {
            return Err(anyhow!("Block already stored"));
        }

        let msg = bincode::serialize(&(block, actions))?;
        // Writes the header if its not already set
        if self.header.block_count <= 0 {
            let header_bytes = self.header.to_vec();
            self.file.seek(SeekFrom::Start(0));
            self.file.write(&header_bytes);
        }
        self._update(&msg);
        self._update_header(block_level, actions_count);
        Ok((block_level + 1))
    }

    fn _update_header(&mut self, block_level: u32, actions_count: u32) {
        self.header.block_height = block_level;
        self.header.actions_count += actions_count;
        self.header.block_count += 1;

        let header_bytes = self.header.to_vec();
        self.file.seek(SeekFrom::Start(0));
        self.file.write(&header_bytes);
    }

    fn _fetch_header(&mut self) {
        self.file.seek(SeekFrom::Start(0));
        let mut h = [0_u8; HEADER_LEN];
        self.file.read_exact(&mut h);
        self.header = ActionsFileHeader::from(h);
    }

    pub fn _update(&mut self, data: &[u8]) {
        self.file.seek(SeekFrom::End(0));
        let header = (data.len() as u32).to_be_bytes();
        let mut dt = vec![];
        dt.extend_from_slice(&header);
        dt.extend_from_slice(data);
        self.file.write(dt.as_slice());
    }
}
