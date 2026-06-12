# SAP HANA CDC Architecture — Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SAP HANA 2.00.088 (SPS08)                           │
│                                                                              │
│  ┌──────────────┐   DML    ┌─────────────────────────────────────────────┐ │
│  │   Client     │ ──────→  │           Indexserver (port 39017)          │ │
│  │  (SQL/JDBC)  │          │                                              │ │
│  └──────────────┘          │  ┌─────────────┐   ┌────────────────────┐  │ │
│                             │  │  L1 Delta   │   │  WAL Logger        │  │ │
│                             │  │ (in-memory) │──→│  (log buffers)     │  │ │
│                             │  └─────────────┘   └────────┬───────────┘  │ │
│                             │                             │               │ │
│                             │                    flush on savepoint       │ │
│                             │                             │               │ │
│                             └─────────────────────────────┼───────────────┘ │
└───────────────────────────────────────────────────────────┼─────────────────┘
                                                            │
                                                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Log Volume  (/hana/mounts/log/mnt00001/)                 │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  logsegment_000_directory.dat  (shadow-paged segment index)         │   │
│  │                                                                      │   │
│  │  Page 0 (physical A):  [Magic|CMax=1|EntryCount=10240|Generation=N] │   │
│  │  Page 1 (physical B):  [Magic|CMax=1|EntryCount=10240|Generation=M] │   │
│  │     └── Winner: whichever has higher Generation with valid checksum  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐          │
│  │ logsegment_      │  │ logsegment_       │  │ logsegment_      │  ...     │
│  │ 000_00000001.dat │  │ 000_00000002.dat  │  │ 000_00000003.dat │          │
│  │                  │  │                   │  │                  │          │
│  │ 16384 × 4KB pages│  │ 16384 × 4KB pages │  │ 16384 × 4KB pages│          │
│  │ MinLSN=4666368   │  │ MinLSN=4639872    │  │ MinLSN=4652672   │          │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘          │
│      ↑ WARNING: files are NOT in LSN order by filename (circular buffer!)   │
│      Sort by MinLSN (page offset 24) to read in correct order               │
└─────────────────────────────────────────────────────────────────────────────┘

                              Page Structure (4096 bytes)
                              ════════════════════════════

  Offset  Size  Field               Confirmed Value (HANA 2.00.088 SPS08)
  ──────  ────  ──────────────────  ──────────────────────────────────────
  0       8     PageMagic           0x00000060FF400002  (constant on all pages)
  8       8     Sentinel            0x7FFFFFFFFFFFFFFF  (INT64_MAX, constant)
  16      8     CurrentPageLSN      monotonically +64 per page
  24      8     SegmentMinLSN       constant per segment  ◄── sort key for ordering
  32      16    SAPTag+Checksum     starts with ASCII "SAP" (0x53 0x41 0x50)
  48      8     DBName+Info         starts with ASCII "HXE" (0x48 0x58 0x45)
  56      8     NextPageLSN         = next page's CurrentPageLSN
  64      4     HanaPropChecksum32  HANA proprietary 32-bit (pg0=0xd5ff3706, pg1=0xd73072af, pg2=0xd861abe0)
  68      4     SegmentInternalID   constant per segment
  72      2     FormatVersion       3
  74      2     PageIndex           0-based, 0–16383
  76      4     UsedBytes           payload bytes after 80-byte header
  80      ...   Block entries       ◄── CDC entries start here

  └──────────────── 80-byte header ──────────────────┘└──── payload ────┘


                         Block Entry (starting at offset 80)
                         ════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────┐
  │             Block Header — 4 bytes (INSERT form confirmed)       │
  │  [type:1][flags:1][col_count:1][row_count:1]                    │
  │  INSERT=0x81, DELETE=0xFE, COMMIT=0xC8, Infrastructure=0x02    │
  └─────────────────────────────────────────────────────────────────┘
  ┌─────────────────────────────────────────────────────────────────┐
  │                    Block Payload                                  │
  │  INSERT(0x81): [type][0x00][col_count][row_count] then          │
  │                 descriptor 49 20 54 04 preceding 4-byte values  │
  │                 RowID uint64 LE located 40 bytes before col vals │
  │  DELETE(0xFE): [0xFE][0x00][RowID:uint64 LE]                   │
  │  UPDATE:       DELETE(0xFE) + INSERT(0x81) — no separate type   │
  │  UPSERT new:   0x81 (same as INSERT)                            │
  │  COMMIT(0xC8): 16-byte block after INT64_MAX sentinel           │
  │  Savepoint:    Infrastructure type 0x02 at page payload start   │
  └─────────────────────────────────────────────────────────────────┘

  Column Value Encoding (confirmed for 4-byte types; VARCHAR/DECIMAL encoding inferred):
  ┌───────────┬───────────────────────────────────────────────────────┐
  │ Type      │ Encoding                                              │
  ├───────────┼───────────────────────────────────────────────────────┤
  │ INTEGER   │ 4 bytes little-endian signed (confirmed: 4-byte LE)  │
  │ TINYINT   │ 4 bytes little-endian int32 (confirmed value encoding)│
  │ BIGINT    │ 8 bytes little-endian signed (inferred)               │
  │ BOOLEAN   │ 1 byte (0x00=false, 0x01=true) (inferred)            │
  │ VARCHAR   │ [len:uint16 LE] + UTF-8 bytes (inferred)             │
  │ DECIMAL(p,s)│ scaled int32 LE ×10^s (inferred)                  │
  │ NULL      │ bit in null bitmap (no value bytes)                   │
  └───────────┴───────────────────────────────────────────────────────┘
  Note: ColumnType code bytes in the `49 20 54 04` descriptor are NOT yet decoded to
  individual column type codes — only VALUE encodings confirmed for 4-byte types.


                     CDC Architecture (saphana_cdc connector)
                     ════════════════════════════════════════

  OPTION A — Trigger-Based CDC (PRODUCTION, currently implemented)
  ────────────────────────────────────────────────────────────────

  HANA Table            Trigger              _RPCN_CDC.CHANGES         saphana_cdc
  ┌─────────┐   INSERT ┌──────────────┐   ┌─────────────────────┐   ┌──────────┐
  │         │ ──────→  │ AFTER INSERT │──→│ ID | OP | NEW_VALUES│──→│          │
  │  user   │   UPDATE │ AFTER UPDATE │   │ (BIGINT IDENTITY PK)│   │  poller  │──→ Debezium
  │  table  │ ──────→  │ AFTER UPDATE │──→│ ID | OP | OLD+NEW   │   │  (Go)    │    JSON
  │         │   DELETE │ AFTER DELETE │   │                     │   │          │
  │         │ ──────→  │ AFTER DELETE │──→│ ID | OP | OLD_VALUES│   └──────────┘
  └─────────┘          └──────────────┘   └─────────────────────┘
                                            LogPos = BIGINT ID
                                            checkpoint persisted to
                                            _RPCN_CDC.CHECKPOINT

  OPTION B — Log-Based CDC (RESEARCH, in development)
  ────────────────────────────────────────────────────

  HANA WAL files        logscanner/page     empirical tests         cdc reader
  ┌──────────────┐      ┌─────────────┐    ┌──────────────────┐    ┌─────────┐
  │logsegment_   │      │ Parse()     │    │ TestDirect*      │    │(future) │
  │000_*.dat     │─────→│ 4KB pages   │───→│ confirm offsets  │───→│ block   │
  │              │      │ offset 16   │    │ detect magic     │    │ parser  │
  │ ~64MB each   │      │ = LSN ✅    │    │ verify stride    │    │         │
  └──────────────┘      └─────────────┘    └──────────────────┘    └─────────┘
  sorted by MinLSN      logformat/blocks/   VERSION_MATRIX.md       Block type codes
  (offset 24) — NOT     column encoding     tracks all findings     confirmed:
  by filename (log is   (4-byte confirmed)                          INSERT=0x81
  a circular buffer;                                                DELETE=0xFE
  filename order ≠                                                  COMMIT=0xC8
  LSN order)                                                        Infra=0x02


                         Debezium Message Envelope
                         ═════════════════════════

  {
    "before": { "ID": 1, "NAME": "Alice" },   ← populated for UPDATE/DELETE
    "after":  { "ID": 1, "NAME": "Bob"   },   ← populated for INSERT/UPDATE
    "source": {
      "connector": "redpanda.saphana",
      "schema":    "HR",
      "table":     "EMPLOYEES",
      "lsn":       "42",                       ← change table sequence ID
      "snapshot":  "false"
    },
    "op":    "u",                               ← c/u/d/r/hb
    "ts_ms": 1749433200000
  }
```
