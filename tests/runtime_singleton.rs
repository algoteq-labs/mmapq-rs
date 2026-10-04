use std::process::Command;

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
