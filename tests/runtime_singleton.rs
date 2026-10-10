use std::process::Command;

/// True if any member of the ar archive at `path` starts with the LLVM
/// bitcode magic (`BC\xC0\xDE`). Native (ELF) members never do, so mixed
/// archives are safe. Member names are not needed, so the GNU string-table
/// format is irrelevant; unreadable, malformed, or thin archives yield false.
fn rlib_contains_bitcode(path: &std::path::Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut magic = [0u8; 8];
    if file.read_exact(&mut magic).is_err() || &magic != b"!<arch>\n" {
        return false;
    }
    loop {
        let mut header = [0u8; 60];
        if file.read_exact(&mut header).is_err() {
            return false;
        }
        let size: u64 = match String::from_utf8_lossy(&header[48..58]).trim().parse() {
            Ok(size) => size,
            Err(_) => return false,
        };
        let mut tag = [0u8; 4];
        if size >= 4 {
            if file.read_exact(&mut tag).is_err() {
                return false;
            }
            if tag == [b'B', b'C', 0xC0, 0xDE] {
                return true;
            }
        }
        let consumed = if size >= 4 { 4 } else { size };
        // Members are 2-byte aligned; skip the rest of this member's data.
        let remaining = size - consumed + (size & 1);
        if file.seek(SeekFrom::Current(remaining as i64)).is_err() {
            return false;
        }
    }
}

fn check_reference_balance(name: &str) {
    let dependencies = std::env::current_exe().unwrap().parent().unwrap().to_owned();
    let library = std::fs::read_dir(&dependencies).unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.file_name().unwrap().to_str().unwrap().starts_with("libmmapq-")
            && path.extension().is_some_and(|extension| extension == "rlib"))
        .max_by_key(|path| std::fs::metadata(path).unwrap().modified().unwrap()).unwrap();
    let directory = std::env::temp_dir().join(format!(
        "mmapq-singleton-{name}-{}", std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let binary = directory.join("probe");
    let mut compiler = Command::new("rustc");
    compiler.args(["--edition=2021"])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/singleton_reference_probe.rs"))
        .arg("--extern").arg(format!("mmapq={}", library.display()))
        .arg("-L").arg(format!("dependency={}", dependencies.display()))
        .arg("-o").arg(&binary);
    // The release profile builds with linker-plugin-lto: the rlib's Rust
    // object is pure LLVM bitcode, which GNU ld rejects ("file format not
    // recognized"). -C lto lets the probe's rustc link from bitcode. Gate on
    // the artifact, not the profile: release with LTO off produces ELF
    // objects -C lto cannot consume, and a debug-assertions proxy would miss
    // release builds with debug-assertions = true. Note rustc >= 1.90 masks
    // this on x86_64 Linux, where rust-lld links bitcode itself.
    if rlib_contains_bitcode(&library) {
        compiler.arg("-C").arg("lto");
    }
    for function in ["region_mapper", "metrics_processor", "clone", "destroy"] {
        compiler.arg("-C").arg(format!("link-arg=-Wl,--wrap=mmapq_runtime_{function}"));
    }
    let compiled = compiler.output().unwrap();
    assert!(compiled.status.success(), "{}", String::from_utf8_lossy(&compiled.stderr));
    let result = Command::new(&binary).arg(name).output().unwrap();
    std::fs::remove_dir_all(directory).unwrap();
    assert!(result.status.success(), "{name}: {}", String::from_utf8_lossy(&result.stderr));
}

#[test]
fn region_mapper_references_balance() {
    check_reference_balance("region_mapper");
}

#[test]
fn metrics_processor_references_balance() {
    check_reference_balance("metrics_processor");
}
