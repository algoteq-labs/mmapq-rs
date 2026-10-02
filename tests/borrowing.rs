use std::path::PathBuf;
use std::process::Command;

#[test]
fn borrowing_contracts() {
    let dependencies = std::env::current_exe().unwrap().parent().unwrap().to_owned();
    let library = std::fs::read_dir(&dependencies).unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.file_name().unwrap().to_str().unwrap().starts_with("libmmapq-")
            && path.extension().is_some_and(|extension| extension == "rlib"))
        .max_by_key(|path| std::fs::metadata(path).unwrap().modified().unwrap()).unwrap();
    let directory = std::env::temp_dir().join(format!("mmapq-borrows-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let legacy = std::env::var_os("MMAPQ_TEST_LEGACY_BORROWS").is_some();
    let mut cases: Vec<(String, String, Option<&str>, bool)> = Vec::new();
    for operation in ["read(1)", "read_first()", "read_last()", "entry_status(1)", "last_index()", "start_index()"] {
        cases.push((format!("reader_{}", cases.len()), format!(
            "fn check(reader: &mut mmapq::IndexedReader) {{ let message = reader.read(0).unwrap().unwrap(); let bytes = message.data(); let _ = reader.{operation}; std::hint::black_box(bytes); }}"
        ), Some("E0499"), true));
    }
    cases.extend([
        ("transfer".into(), "fn check(reader: &mut mmapq::IndexedReader) { let message = reader.read(0).unwrap().unwrap(); let _ = mmapq::transfer::QueueTransfer::export(reader, \"unused\", 0, 1, mmapq::transfer::StartIndexMode::Rebase); std::hint::black_box(message.data()); }".into(), Some("E0499"), true),
        ("batch_step".into(), "fn check(cursor: &mut mmapq::batch::BatchMessageIter<'_>) { let first = cursor.next(); let _ = cursor.next(); std::hint::black_box(first); }".into(), Some("E0499"), true),
        ("batch_reset".into(), "fn check(batch: &mut mmapq::batch::BatchMessage<'_>) { let mut first = batch.iter(); let _ = batch.iter(); std::hint::black_box(first.next()); }".into(), Some("E0499"), true),
        ("batch_reader".into(), "fn check(reader: &mut mmapq::batch::IndexedBatchReader) { let batch = reader.read(0).unwrap().unwrap(); let _ = reader.read(1); std::hint::black_box(batch.index()); }".into(), Some("E0499"), true),
        ("bulk_copy".into(), "fn check(writer: &mut mmapq::batch::BatchWriter<'_>, cursor: &mut mmapq::batch::BatchMessageIter<'_>) { let first = cursor.next(); let _ = writer.append_remaining_from_iter(cursor); std::hint::black_box(first); }".into(), Some("E0499"), true),
        ("drop_owner".into(), "fn check(mut reader: mmapq::IndexedReader) { let message = reader.read(0).unwrap().unwrap(); drop(reader); std::hint::black_box(message.data()); }".into(), Some("E0505"), false),
        ("escape_callback".into(), "fn check(poller: &mut mmapq::batch::IndexedBatchPoller) { let mut escaped = None; let _ = poller.poll(|_, cursor| { escaped = Some(cursor); mmapq::NextMove::Stop }); std::hint::black_box(escaped); }".into(), Some("E0521"), false),
        ("positive_reader".into(), "fn check(reader: &mut mmapq::IndexedReader) { { let message = reader.read(0).unwrap().unwrap(); std::hint::black_box(message.data()); } let _ = reader.read(1); }".into(), None, false),
        ("positive_cursor".into(), "fn check(cursor: &mut mmapq::batch::BatchMessageIter<'_>) { std::hint::black_box(cursor.next()); std::hint::black_box(cursor.next()); }".into(), None, false),
    ]);
    for kind in ["mmapq::IndexedReader", "mmapq::batch::IndexedBatchReader", "mmapq::reader::Message<'static>", "mmapq::batch::BatchMessage<'static>", "mmapq::batch::BatchMessageIter<'static>"] {
        for bound in ["Send", "Sync"] {
            cases.push((format!("thread_{}", cases.len()), format!("fn require_bound<T: {bound}>() {{}} fn check() {{ require_bound::<{kind}>(); }}"), Some("E0277"), false));
        }
    }
    for (name, source, diagnostic, accepted_before) in cases {
        let input: PathBuf = directory.join(format!("{name}.rs"));
        std::fs::write(&input, source).unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "--crate-type=lib", "--emit=metadata"])
            .arg(&input).arg("--out-dir").arg(&directory)
            .arg("--extern").arg(format!("mmapq={}", library.display()))
            .arg("-L").arg(format!("dependency={}", dependencies.display()))
            .output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        if diagnostic.is_none() || (legacy && accepted_before) {
            assert!(output.status.success(), "{name}: {stderr}");
        } else {
            assert!(!output.status.success(), "{name} unexpectedly compiled");
            assert!(stderr.contains(diagnostic.unwrap()), "{name}: {stderr}");
        }
    }
    std::fs::remove_dir_all(directory).unwrap();
}
