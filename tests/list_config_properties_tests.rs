use std::fs;
use std::process::Command;

#[test]
fn list_config_properties_prints_all_values_unmasked_in_key_order() {
    let data_dir = tempfile::tempdir().unwrap();
    let pds_dir = data_dir.path().join("pds");
    fs::create_dir(&pds_dir).unwrap();
    let db = rusqlite::Connection::open(pds_dir.join("pds.db")).unwrap();
    db.execute_batch(
        "CREATE TABLE ConfigProperty (Key TEXT PRIMARY KEY NOT NULL, Value TEXT NOT NULL);
         INSERT INTO ConfigProperty VALUES ('UserHandle', 'alice.example');
         INSERT INTO ConfigProperty VALUES ('AzureStorageSASURL', 'https://example.test/?sig=fake');
         INSERT INTO ConfigProperty VALUES ('ArbitraryProperty', '');",
    )
    .unwrap();
    drop(db);

    let output = Command::new(env!("CARGO_BIN_EXE_rustproto"))
        .args(["/command", "ListConfigProperties", "/dataDir"])
        .arg(data_dir.path())
        .output()
        .unwrap();

    assert!(output.status.success(), "{:?}", output);
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "ArbitraryProperty=\nAzureStorageSASURL=https://example.test/?sig=fake\nUserHandle=alice.example\n"
    );
    assert_eq!(fs::read_dir(data_dir.path()).unwrap().count(), 1);
}

#[test]
fn list_config_properties_fails_without_creating_a_missing_database() {
    let data_dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rustproto"))
        .args(["/command", "list_config_properties", "/dataDir"])
        .arg(data_dir.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Failed to connect to PDS database"));
    assert!(!data_dir.path().join("pds").exists());
}
