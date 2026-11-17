Schema v1: users, audit, datasets (JSON write sequence), configs (bounded LSM parameters), trials (immutable input snapshot and JSON result). MySQL stores metadata only. Run python -m scripts.seed_demo for create-if-missing and natural-key seed; existing values are not overwritten.

Schema v2 (archive): three tables for offline backup/restore, created automatically by the application on startup (Base.metadata.create_all):

- backup_records: one row per produced package (full or incremental). sequence is a single monotonic chain; the full package is sequence 1 and every incremental points at its parent_id. Watermarks hold the largest included object id per entity, so objects created after a package's snapshot are left for the next package.
- restore_jobs: state machine pending -> applying -> available | rejected. A rejected whole package leaves no business data behind; an interrupted apply rolls back and returns to pending. Re-applying an available job is a no-op (no duplicate inserts).
- archive_objects: content-addressable ledger of (object_type, content_sha256, local_id). Unique per content hash; it powers same-name/same-content reuse, same-name/different-content rename-as-copy, and idempotent retries after interruption.

Package format `lsm-archive/v1` is a single JSON document: manifest (object fingerprint index, parent chain, watermarks) + payload (full dataset/config/completed-trial contents) + a SHA-256 signature over canonicalized manifest and payload. Verification rejects tampered bytes, missing ancestors, discontinuous chains and unresolved trial references by refusing the whole package.
