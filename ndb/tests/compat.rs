use ndb::{from_ndb_note, to_ndb_note, NdbNote};

const TEST_EVENT_JSON: &str = r#"{
  "content": "hello world",
  "created_at": 1759565037,
  "id": "65562cc9fc1636c21268b4d57a13168cac03030d0a98ab33b9a29b6333cb093b",
  "kind": 1,
  "pubkey": "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
  "sig": "cd9be669759ac9a17e029d2e5de53d6b61e86326f7d854ddc47dd3e66b358bc40d0fd68b0685b58ee2e5f46b1e530b40a52d45dbed7a5690c3e36e65048c8609",
  "tags": [
    [
      "t",
      "test"
    ],
    [
      "t",
      "hello"
    ],
    [
      "alt",
      "alt text"
    ]
  ]
}"#;

const TEST_EVENT_NOTE: &[u8] = include_bytes!("fixtures/test-event.ndb");

#[test]
fn binary_compatibility_with_nostrdb() -> Result<(), Box<dyn std::error::Error>> {
    let encoded = to_ndb_note(TEST_EVENT_JSON)?;
    assert_eq!(encoded.as_slice(), TEST_EVENT_NOTE, "Rust encoder must match nostrdb output");

    let decoded = from_ndb_note(TEST_EVENT_NOTE)?;

    let original_value: serde_json::Value = serde_json::from_str(TEST_EVENT_JSON)?;
    let decoded_value: serde_json::Value = serde_json::from_str(&decoded)?;
    assert_eq!(decoded_value, original_value, "Decoded JSON differs from original");

    let note = NdbNote::from_bytes(TEST_EVENT_NOTE)?;
    assert_eq!(note.version(), 1);
    assert_eq!(note.kind(), original_value["kind"].as_u64().unwrap() as u32);
    assert_eq!(note.content_str()?, original_value["content"].as_str().unwrap());
    assert_eq!(note.tags().len(), original_value["tags"].as_array().unwrap().len());

    Ok(())
}
