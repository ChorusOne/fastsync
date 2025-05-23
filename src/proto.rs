use borsh::{BorshDeserialize, BorshSerialize};

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum FileStatus {
    Missing,
    UpToDate,
    NeedsSync { len: u64, mtime: u64 },
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct ManifestReply {
    pub files: Vec<FileStatus>,
}
