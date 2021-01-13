use crate::schema::KeyValueSchema;
use crate::ivec::IVec;
use crate::database::DB;


/// Database iterator direction
///
#[derive(Clone)]
pub enum Direction {
    Forward,
    Reverse,
}

#[derive(Clone)]
pub enum IteratorMode {
    Start,
    End,
    From(IVec, Direction),
}

pub struct DBIterator<'a> {
    raw: &'a DB,
    mode: IteratorMode,
}

impl<'a> DBIterator<'a> {
    pub(crate) fn new(raw: &'a DB, mode: IteratorMode) -> Self {
        DBIterator {
            raw,
            mode,
        }
    }
}


impl<'a> Iterator for DBIterator<'a> {
    type Item = (IVec, IVec);

    fn next(&mut self) -> Option<Self::Item> {
        match &self.mode {
            IteratorMode::Start => {
                self.raw.inner.iter().next().map(|(k, v)| { (k.clone(), v.clone()) })
            }
            IteratorMode::End => {
                self.raw.inner.iter().last().map(|(k, v)| { (k.clone(), v.clone()) })
            }
            IteratorMode::From(k, direction) => {
                let key = k.to_vec();
                match direction {
                    Direction::Forward => {
                        self.raw.inner.range(IVec::from(key)..).next().map(|(k, v)| { (k.clone(), v.clone()) })
                    }
                    Direction::Reverse => {
                        self.raw.inner.range(IVec::from(key)..).last().map(|(k, v)| { (k.clone(), v.clone()) })
                    }
                }
            }
        }
    }
}

pub trait DBIterationHandler {
    fn iter(&self, mode: IteratorMode) -> DBIterator;
    fn scan_prefix(&self, prefix: &[u8]) -> DBIterator;
}

impl DBIterationHandler for DB {
    fn iter(&self, mode: IteratorMode) -> DBIterator {
        DBIterator::new(self, mode)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> DBIterator {
        DBIterator::new(self, IteratorMode::From(IVec::from(prefix), Direction::Forward))
    }
}