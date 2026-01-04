# udbctl - UnisonDB Control CLI

`udbctl` is a command-line tool for UnisonDB.

## Installation

```bash
go install github.com/ankur-anand/unisondb/cmd/udbctl@latest
```

Or build from source:

```bash
go build -o udbctl ./cmd/udbctl
```

## Global Options

| Flag | Alias | Description |
|------|-------|-------------|
| `--help` | `-h` | Show help |
| `--version` | `-v` | Print version |

## Commands

### WAL Commands

WAL inspection commands are read-only and safe to run while the server is running.

---

#### `udbctl wal list`

List all WAL segments with metadata.

**Usage:**
```bash
udbctl wal list --wal-dir <path> [--format <format>]
```

**Flags:**

| Flag | Alias | Required | Description |
|------|-------|----------|-------------|
| `--wal-dir` | `-w` | Yes | Path to WAL directory |
| `--format` | `-f` | No | Output format: `table` (default) or `json` |

**Example:**
```bash
$ udbctl wal list -w /var/lib/unisondb/default/wal

ID        STATUS  SIZE     ENTRIES  FIRST_INDEX  LAST_MODIFIED
00000001  Sealed  64 MB    52,431   1            2024-01-15T10:30:00Z
00000002  Sealed  64 MB    51,892   52,432       2024-01-15T11:45:00Z
00000003  Active  32 MB    25,120   104,324      2024-01-15T12:00:00Z
```

**JSON Output:**
```bash
$ udbctl wal list -w /var/lib/unisondb/default/wal -f json

[
  {
    "id": 1,
    "status": "Sealed",
    "size": 67108864,
    "size_human": "64 MB",
    "entry_count": 52431,
    "first_log_index": 1,
    "last_modified": "2024-01-15T10:30:00Z"
  },
  ...
]
```

---

#### `udbctl wal inspect`

Show detailed information for a specific WAL segment.

**Usage:**
```bash
udbctl wal inspect --wal-dir <path> --segment-id <id> [--show-index] [--format <format>]
```

**Flags:**

| Flag | Alias | Required | Description |
|------|-------|----------|-------------|
| `--wal-dir` | `-w` | Yes | Path to WAL directory |
| `--segment-id` | `-s` | Yes | Segment ID to inspect |
| `--show-index` | | No | Include index entries in output |
| `--format` | `-f` | No | Output format: `table` (default) or `json` |

**Example:**
```bash
$ udbctl wal inspect -w /var/lib/unisondb/default/wal -s 3

Segment Details
===============
ID:             3
Status:         Active
Size:           32 MB (33,685,504 bytes)
Write Offset:   33,685,120
Entry Count:    25,120
First LogIndex: 104,324
Flags:          0x01
Last Modified:  2024-01-15T12:30:00Z
```

**With Index Entries:**
```bash
$ udbctl wal inspect -w /var/lib/unisondb/default/wal -s 3 --show-index

Segment Details
===============
ID:             3
Status:         Active
Size:           32 MB (33,685,504 bytes)
Write Offset:   33,685,120
Entry Count:    25,120
First LogIndex: 104,324
Flags:          0x01
Last Modified:  2024-01-15T12:30:00Z

Index Entries (25,120 total):
  #      OFFSET   LENGTH
  1      64       1,024
  2      1,088    2,048
  3      3,136    512
  4      3,648    1,536
  5      5,184    768
  6      5,952    2,048
  7      8,000    1,024
  8      9,024    512
  9      9,536    1,536
  10     11,072   768
  ...
```

---

#### `udbctl wal stats`

Show aggregate statistics across all WAL segments.

**Usage:**
```bash
udbctl wal stats --wal-dir <path> [--format <format>]
```

**Flags:**

| Flag | Alias | Required | Description |
|------|-------|----------|-------------|
| `--wal-dir` | `-w` | Yes | Path to WAL directory |
| `--format` | `-f` | No | Output format: `table` (default) or `json` |

**Example:**
```bash
$ udbctl wal stats -w /var/lib/unisondb/default/wal

WAL Statistics
==============
Total Segments:    3
  Sealed:          2
  Active:          1
Total Entries:     129,443
Total Size:        160 MB
Log Index Range:   1 - 129,443
```

**JSON Output:**
```bash
$ udbctl wal stats -w /var/lib/unisondb/default/wal -f json

{
  "total_segments": 3,
  "sealed_count": 2,
  "active_count": 1,
  "total_entries": 129443,
  "total_size": 167772160,
  "total_size_human": "160 MB",
  "first_log_index": 1,
  "last_log_index": 129443
}
```

---

### Restore Command

Restore B-Tree and/or WAL from backup files. **Requires the server to be stopped.**

#### `udbctl restore`

**Usage:**
```bash
udbctl restore --data-dir <path> --namespace <name> [--btree <file>] [--wal <dir>] [--dry-run | --force]
```

**Flags:**

| Flag | Alias | Required | Description |
|------|-------|----------|-------------|
| `--data-dir` | `-d` | Yes | Target data directory |
| `--namespace` | `-n` | Yes | Namespace to restore |
| `--btree` | | No* | Path to B-Tree backup file |
| `--wal` | | No* | Path to WAL backup directory |
| `--dry-run` | | No | Validate backup files without restoring |
| `--force` | | No** | Confirm server is stopped |
| `--format` | `-f` | No | Output format: `table` (default) or `json` |

*At least one of `--btree` or `--wal` must be specified.

**`--force` is required for actual restore operations (not needed for `--dry-run`).

**Safety Features:**

- **Exclusive Lock**: Acquires `pid.lock` during the entire restore operation, preventing the server from starting and blocking concurrent restore attempts
- **Dry Run Mode**: Use `--dry-run` to validate backup files and preview what would be restored without modifying any data
- **Force Flag**: Requires explicit `--force` flag for actual restores to confirm intent

---

#### Dry Run Example

Preview what would be restored without making any changes:

```bash
$ udbctl restore \
    --data-dir /var/lib/unisondb \
    --namespace default \
    --btree /backup/2024-01-15/btree.snapshot \
    --wal /backup/2024-01-15/wal/ \
    --dry-run

[DRY RUN] The following would be restored:

Would restore B-Tree: /var/lib/unisondb/default/unison.db/data.mdb (256 MB)
Would restore 5 WAL segments to /var/lib/unisondb/default/wal

Run without --dry-run to perform the restore.
```

---

#### Restore Examples

**Restore B-Tree only:**
```bash
$ udbctl restore \
    --data-dir /var/lib/unisondb \
    --namespace default \
    --btree /backup/2024-01-15/btree.snapshot \
    --force

WARNING: This will overwrite existing data!
  Data Dir:  /var/lib/unisondb
  Namespace: default

Restored B-Tree: /var/lib/unisondb/default/unison.db/data.mdb (256 MB)

Restore completed successfully!
```

**Restore WAL only:**
```bash
$ udbctl restore \
    --data-dir /var/lib/unisondb \
    --namespace default \
    --wal /backup/2024-01-15/wal/ \
    --force

WARNING: This will overwrite existing data!
  Data Dir:  /var/lib/unisondb
  Namespace: default

Restored 5 WAL segments to /var/lib/unisondb/default/wal/

Restore completed successfully!
```

**Full Restore:**
```bash
$ udbctl restore \
    --data-dir /var/lib/unisondb \
    --namespace default \
    --btree /backup/2024-01-15/btree.snapshot \
    --wal /backup/2024-01-15/wal/ \
    --force

WARNING: This will overwrite existing data!
  Data Dir:  /var/lib/unisondb
  Namespace: default

Restored B-Tree: /var/lib/unisondb/default/unison.db/data.mdb (256 MB)
Restored 5 WAL segments to /var/lib/unisondb/default/wal/

Restore completed successfully!
```

---

## Common Workflows

### Inspecting WAL Health

```bash
# Check overall WAL statistics
udbctl wal stats -w /var/lib/unisondb/default/wal

# List all segments to identify issues
udbctl wal list -w /var/lib/unisondb/default/wal

# Inspect a specific segment in detail
udbctl wal inspect -w /var/lib/unisondb/default/wal -s 3 --show-index
```

### Disaster Recovery

```bash
# 1. Stop the UnisonDB server
systemctl stop unisondb

# 2. Preview what will be restored (optional but recommended)
udbctl restore --data-dir /var/lib/unisondb --namespace default \
    --btree /backup/btree.mdb --wal /backup/wal/ --dry-run

# 3. Perform the restore
udbctl restore --data-dir /var/lib/unisondb --namespace default \
    --btree /backup/btree.mdb --wal /backup/wal/ --force

# 4. Start the server
systemctl start unisondb
```

### Exporting Data as JSON

```bash
# Export segment list to JSON for processing
udbctl wal list -w /var/lib/unisondb/default/wal -f json > segments.json
```
---

## File Locations

| Component | Default Path |
|-----------|--------------|
| WAL Directory | `<data-dir>/<namespace>/wal/` |
| B-Tree Database | `<data-dir>/<namespace>/unison.db/data.mdb` |
| PID Lock File | `<data-dir>/<namespace>/pid.lock` |
