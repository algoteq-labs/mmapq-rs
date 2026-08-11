fn main() {
    let c_src = "c/src";
    let c_inc = "c/include";

    let files: &[&str] = &[
        "appender_id_pool.c", "appender_meta.c", "batch_payload_iterator.c",
        "batch_queue_latency_probe.c", "batch_queue_transfer.c",
        "file_initialiser.c", "file_mapper_rolled.c", "file_mapper_single_ro.c",
        "file_mapper_single_rw.c", "fixed_size_region.c", "header_reader.c",
        "indexed_appender.c", "indexed_archiver.c", "indexed_batch_appender.c",
        "indexed_batch_archiver.c", "indexed_batch_poller.c", "indexed_batch_queue.c",
        "indexed_batch_reader.c", "indexed_poller.c", "indexed_queue.c",
        "indexed_reader.c", "payload_iterator.c", "payload_walk.c", "queue_files.c",
        "queue_latency_probe.c", "queue_meta.c", "queue_region_accessor.c",
        "queue_transfer.c", "region_async.c", "region_ring.c",
        "region_ring_accessor.c", "region_sync.c", "runtime.c",
        "uncommitted_payload_iterator.c",
    ];

    let paths: Vec<String> = files.iter().map(|f| format!("{}/{}", c_src, f)).collect();
    let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();

    let mut build = cc::Build::new();
    build
        .files(&refs)
        .include(c_inc)
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
