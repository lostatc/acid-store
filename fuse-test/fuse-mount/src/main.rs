use acid_store::repo::{
    file::{Entry, FileRepo, UnixMetadata, UnixSpecialType},
    OpenMode, OpenOptions,
};
use acid_store::store::MemoryConfig;
use std::env;
use std::path::Path;

fn main() {
    let args = env::args().collect::<Vec<_>>();
    let mount_path = Path::new(
        args.get(1)
            .expect("First argument should be the mount point."),
    );

    let config = MemoryConfig::new();
    let mut repo: FileRepo<UnixSpecialType, UnixMetadata> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&config)
        .unwrap();

    repo.create("root", &Entry::directory()).unwrap();
    repo.mount(mount_path, "root", &["-o", "auto_unmount", "-o", "allow_other"]).unwrap();
}
