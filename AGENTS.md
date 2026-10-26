# fasthep-carpenter Agent Instructions

These instructions apply when working in the standalone `fasthep-carpenter`
repository. The Python package is `fasthep_carpenter`.

## Ownership

`fasthep-carpenter` owns reusable HEP analysis components for FAST-HEP:

- ROOT sources and sinks built on uproot;
- Awkward-array event-stream transforms;
- object selections, overlap cleaning, pair/recoil/candidate builders and
  expression-backed definitions;
- histogram filling, weights, cutflows and materialized product handlers;
- HEP/CMS-oriented reusable operations where they are still experiment-agnostic
  enough to share;
- runtime helpers and execution modifiers for analysis operations.

It does not own the backend-neutral workflow engine:

- `fasthep-flow` owns workflow YAML, normalisation, graph/data-flow analysis,
  execution plans, runtime orchestration, registries and backend interfaces.
  Carpenter provides specs and implementations consumed by Flow.
- `fasthep-curator` owns dataset inspection, schema snapshots, diagnostics and
  metadata/provenance tooling beyond Carpenter's operation products.
- `fasthep-render` owns plotting/report rendering. Carpenter may produce
  histogram or cutflow products, but visual presentation belongs there.
- Experiment-specific analysis repositories own analysis selections, thresholds,
  weights, correction choices and compatibility shims. Do not hide framework
  defects with analysis-specific workarounds in Carpenter.

## Registry Contracts

- `src/fasthep_carpenter/profiles/registry.yaml` registers sources,
  transforms, sinks, execution modifiers, product handlers, functions and
  constants.
- Registered operation modules usually contain both compile-time specs and
  runtime implementations, for example `operations/select_objects.py` exposes
  `SELECT_OBJECTS_SPEC` and `run_select_objects`.
- Keep compile-time specs and runtime implementations consistent: `requires`,
  `provides`, parser hooks, defaults, generated fields and runtime outputs must
  describe the same contract.
- Author-facing parameters and produced field names are public contracts. Change
  them deliberately with focused tests and documentation.
- Registered operation modules must not import implementation helpers from other
  registered operations. Genuinely shared logic belongs in an internal helper
  module such as `operations/_selection.py`, `operations/_validation.py`,
  `runtime/`, or a new private module.

## Important Locations

- Public package: `src/fasthep_carpenter/`
- Registered operations: `src/fasthep_carpenter/operations/`
- CMS-specific operations: `src/fasthep_carpenter/operations/cms/`
- Weight operations: `src/fasthep_carpenter/operations/weights/`
- ROOT source and sink: `src/fasthep_carpenter/sources/root_tree.py`,
  `src/fasthep_carpenter/sinks/root_tree.py`
- Product handlers: `src/fasthep_carpenter/products/`
- Runtime helpers and modifiers: `src/fasthep_carpenter/runtime/`
- Registry profile: `src/fasthep_carpenter/profiles/registry.yaml`
- Tests: `tests/`
- Manual GPU checks: `tests/manual/`
- Documentation source: none currently exists; `pixi.toml` still defines a
  `docs` task for a future `docs/` tree.

## Development Commands

Use Pixi from the repository root.

- Install/update the development environment: `pixi install`
- Format: `pixi run format`
- Lint: `pixi run lint`
- Autofix lint: `pixi run lint-fix`
- Type-check: `pixi run typecheck`
- Test: `pixi run test`
- Package build: `pixi run build`
- Build docs, once `docs/` exists: `pixi run docs`
- Clean built docs: `pixi run docs-clean`
- Standard validation: `pixi run check`
- Release-style validation: `pixi run ci`
- Distribution check after building: `pixi run check-dist`

`check` runs lint, type-check and tests. `ci` runs `check` and then builds the
package. Pixi environments include `py311`, `py313`, `py314`, `gpu` and `docs`;
use environment-qualified runs for Python-version, GPU or docs-specific work.

## Testing Expectations

- Operation changes need focused tests in the matching `tests/test_*.py` file
  covering Awkward runtime behaviour, empty inputs, jagged/scalar shape
  handling, error messages and output field names.
- Expression and dependency-parser changes need coverage in
  `tests/test_dependency_parsers.py` and, when plan shape matters,
  `tests/test_flow_integration.py`.
- Source and sink changes need ROOT round-trip, missing-branch, metadata-only,
  local file and remote URI coverage in `tests/test_flow_integration.py`,
  `tests/test_root_tree_writer.py`, or source-specific tests.
- Histogram, cutflow and materialization changes need product-handler tests in
  `tests/test_hist_transform.py`, `tests/test_cutflow_transform.py` and
  `tests/test_product_handlers.py` as appropriate.
- Registry/profile changes should update `tests/test_imports_and_profile.py`
  so every registered spec and implementation resolves.
- Execution modifier or GPU work should keep ordinary unit tests mockable and
  put hardware-dependent checks in `tests/manual/` or the `gpu` Pixi
  environment.

## Optional Requirements

- Normal tests create small local arrays and ROOT files under temporary
  directories; they should not require CVMFS, experiment data, grid credentials
  or remote storage.
- ROOT/XRootD behaviour depends on `uproot` and `fsspec-xrootd`; live remote
  files, CVMFS resources and experiment datasets are optional integration
  inputs, not baseline unit-test requirements.
- GPU helpers require the `gpu` Pixi environment plus usable CUDA hardware and
  drivers.

## Generated Files

- Do not edit `src/fasthep_carpenter/_version.py` by hand; it is generated by
  `hatch-vcs`. Keep `src/fasthep_carpenter/_version.pyi` aligned if public
  typing changes.
- Build outputs, ROOT outputs, compiled workflow artifacts and generated docs
  are products of tests or workflows. Do not check them in unless they are
  intentional fixtures.

## Documentation and Changelog

User-facing changes — new features, behavior changes, bug fixes, breaking changes — must add an
entry to `CHANGELOG.md` under `## [Unreleased]`, in the matching [Keep a
Changelog](https://keepachangelog.com/en/1.1.0/) category (Added, Changed, Deprecated, Removed,
Fixed, Security). This repository has no `docs/` yet.
