use acid_store::repo::{
    file::{Entry, FileRepo, MountOption, UnixMetadata, UnixSpecial},
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
    let mut repo: FileRepo<UnixSpecial, UnixMetadata> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&config)
        .unwrap();

    repo.create("root", &Entry::directory()).unwrap();
    repo.mount(
        mount_path,
        "root",
        &[MountOption::AutoUnmount, MountOption::AllowOther],
    )
    .unwrap();
}
