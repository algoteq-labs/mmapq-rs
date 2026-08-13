fn main() {
    let c_src = "c/src";
    let c_inc = "c/include";

    // Glob over the mirrored C sources so a new .c file is picked up
    // automatically instead of silently dropping out of the build (which
    // surfaces only as an undefined symbol in a downstream crate). Sorted
    // for deterministic link order. `cargo:rerun-if-changed=c/src` below
    // re-triggers on any addition.
    let mut entries: Vec<_> = std::fs::read_dir(c_src)
        .expect("c/src mirror directory must exist")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "c").unwrap_or(false))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    let paths: Vec<String> = entries
        .iter()
        .map(|e| e.path().to_str().expect("non-UTF-8 path").to_string())
        .collect();
    let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();

    let c_inc_src = "c/src";
    let mut build = cc::Build::new();
    build
        .files(&refs)
        .include(c_inc)
        .include(c_inc_src)
        .define("_GNU_SOURCE", None)
        .warnings_into_errors(false);

    if std::env::var("PROFILE").unwrap_or_default() == "release" {
        build.define("NDEBUG", None);
    }

    build.compile("mmap_queue");

    println!("cargo:rerun-if-changed=c/src");
    println!("cargo:rerun-if-changed=c/include");
    println!("cargo:rerun-if-env-changed=PROFILE");
}
