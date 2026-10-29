# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project uses
calendar versioning (`YYYY.MM.N`) rather than semantic versioning.

Add new entries under `## [Unreleased]`, in the category they belong to. At release time, rename
`## [Unreleased]` to `## [<version>] - <YYYY-MM-DD>` and start a fresh `## [Unreleased]` above it.

## [Unreleased]

### Added

- The `root_tree` writer accepts `shared_counters: true` (TTree only). The jagged branches
  `<collection>_<attribute>` with the same lengths then share one counter `n<collection>`, as the
  legacy skims do, instead of every branch getting its own `n<branch>`.

### Fixed

- Cutflow merging across three or more datasets no longer collapses most datasets into a single
  `"default"` bucket with zeroed stats. `merge_cutflow_products` only correctly read a raw
  single-dataset dict's `"dataset"`/`"cuts"` fields; once hepflow's runtime folds per-dataset
  results into the global cutflow pairwise, the second fold onward combines an already-canonical
  multi-dataset graph (no top-level `"dataset"`/`"cuts"`) with a new raw dict, and only the newest
  dataset survived. Affects any workflow with partitioned/chunked execution across 3+ datasets
  (for example `fasthep-workshop`'s `99-extras/partitioned-root-skim` tutorial).

## [2026.10.1] - 2026-10-26

### Fixed

- `hep.choose` now registers all declared output fields as products, not just the ones a branch
  actually writes.
- Selection: cutflow masks are now combined explicitly instead of relying on implicit combination.

## [2026.08.2] - 2026-08-25

### Added

- `hep.choose` operation for conditional branch selection.
- Dtype casting support in `hep.define`.
- `combine` options for cutflow and histogram operations, so multiple inputs can be merged in one
  step.
- Artifact tracking for the ROOT file writer, recording the files it produces.
- Existing producers can now have an explicit output boundary specified.

### Changed

- `hep.define` and `op.project_fields` support matrix expansion; `project_fields` also gained file
  mapping for its `aliases` parameter.
- `select_objects` now allows missing fields among declared outputs instead of requiring all of
  them to be present.
- Cutflow event-mask handling and error messages improved.
