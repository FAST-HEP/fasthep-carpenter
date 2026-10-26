# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project uses
calendar versioning (`YYYY.MM.N`) rather than semantic versioning.

Add new entries under `## [Unreleased]`, in the category they belong to. At release time, rename
`## [Unreleased]` to `## [<version>] - <YYYY-MM-DD>` and start a fresh `## [Unreleased]` above it.

## [Unreleased]

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
