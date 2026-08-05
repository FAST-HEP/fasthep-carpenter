# fasthep-carpenter

[![CI](https://github.com/FAST-HEP/fasthep-carpenter/actions/workflows/ci.yml/badge.svg)](https://github.com/FAST-HEP/fasthep-carpenter/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/fasthep-carpenter)](https://pypi.org/project/fasthep-carpenter/)
[![Python Versions](https://img.shields.io/pypi/pyversions/fasthep-carpenter)](https://pypi.org/project/fasthep-carpenter/)
[![Documentation Status](https://readthedocs.org/projects/fasthep-carpenter/badge/?version=latest)](https://fasthep-carpenter.readthedocs.io/en/latest/)
[![Discussions](https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github)](https://github.com/FAST-HEP/fasthep/discussions)

<p align="center">
  <a href="https://github.com/FAST-HEP/fasthep">
    <picture>
      <source
        media="(prefers-color-scheme: dark)"
        srcset="https://raw.githubusercontent.com/FAST-HEP/logos-etc/master/fast-hep-white.png"
      >
      <source
        media="(prefers-color-scheme: light)"
        srcset="https://raw.githubusercontent.com/FAST-HEP/logos-etc/master/fast-hep-black.png"
      >
      <img
        alt="FAST-HEP"
        src="https://raw.githubusercontent.com/FAST-HEP/logos-etc/master/fast-hep-black.png"
        width="500"
      >
    </picture>
  </a>
</p>

`fasthep-carpenter` provides common analysis building blocks for FAST-HEP workflows.

It contains reusable High Energy Physics transforms, sources, sinks, and runtime helpers built on top of `fasthep-flow`.

The Python import namespace is:

```python
import fasthep_carpenter
```

## Scope

`fasthep-carpenter` is responsible for:

* ROOT and awkward-array based sources
* event stream manipulation
* HEP analysis transforms
* histogram filling
* cutflows
* object selection helpers
* common CMS/LHC analysis utilities
* workflow runtime extensions

It is the main “analysis implementation” layer of the FAST-HEP ecosystem.

## Relationship to `fasthep-flow`

`fasthep-flow` provides:

* workflow compilation
* execution planning
* orchestration
* registries
* backend interfaces

`fasthep-carpenter` provides:

* concrete analysis operations
* HEP-specific runtime behaviour
* physics object manipulation
* common workflow primitives

In practice, most HEP users will use both packages together.

## Recommended companion packages

* `fasthep-flow`

  * workflow language and execution engine

* `fasthep-curator`

  * dataset inspection
  * schema generation
  * metadata snapshots

* `fasthep-render`

  * plotting
  * tables
  * reports

* `fasthep-cli`

  * the `fasthep` command-line interface

Alternatively, install the meta package:

```bash
pip install fasthep
```

## Installation

Install directly:

```bash
pip install fasthep-carpenter
```

Development environment:

```bash
pixi install
pixi run ci
```

## Minimal example

Example transform registration:

```yaml
registry:
  transforms:
    define:
      spec: fasthep_carpenter.spec.define_transform:DEFINE_TRANSFORM_SPEC
      impl: fasthep_carpenter.impl.define_transform:run_define_transform
```

Example workflow snippet:

```yaml
steps:
  - id: TightMuon
    op: hep.select_objects
    params:
      collection: Muon
      output: selected_tight_Muon
      selection:
        - pt >= 20
        - abs(eta) <= 2.4
      keep:
        - pt
        - eta
        - phi
        - mass
```

`hep.select_objects` evaluates `selection` expressions relative to the input
collection, keeps exactly the configured fields, emits `n<output>` as the
selected-object count, and sorts selected objects by descending `pt` by default.
Use `sort` to override the ordering or `sort: false` to preserve input order.
Overlap removal is intentionally separate and belongs in `hep.clean`.

Use `hep.build_pairs` when an analysis needs explicit pair-candidate products
rather than a first-two-object scalar mass. The operation accepts one or more
input collections, concatenates them in declared order, forms all unordered
pairs, evaluates pair expressions in a `lepton_1_<field>` /
`lepton_2_<field>` context, builds candidate four-vectors, evaluates candidate
expressions in a `pt`/`eta`/`phi`/`mass` context, and can stably sort the
candidate and aligned constituent collections without choosing or truncating to
one candidate. It emits explicit flat output fields plus `n<output>_Z`; use
`hep.selection.flag` on that count when a reusable event flag is needed.
`hep.di_object_mass` remains the simpler operation for a scalar mass from the
first two objects in one collection.

Use `hep.build_lepton_met_candidate` for single-lepton plus MET candidates. It
consumes one lepton collection and one scalar MET product, evaluates lepton
selection expressions relative to the lepton collection, broadcasts MET against
all selected leptons, and writes aligned `<output>_W_*` and `<output>_lepton_*`
collections. The candidate context exposes `pt`, `eta`, `phi`, `mass`, and
`MT`, where `MT` is the transverse mass. Counts are explicit products:
`n<output>_lepton` records the number of leptons after lepton selection, while
`n<output>_W` records the number of candidates after candidate selection. The
operation preserves all surviving candidates and does not filter events; use
`hep.selection.flag` on the count products for reusable event booleans.

Use `hep.selection.flag` for event-level predicates that should be materialized
as boolean fields without filtering events or producing cutflow counts. Its
`selection` list is combined with logical AND and written to `output`; if
`output` is omitted, author normalization fills it from the exact stage id.
Expressions operate on event products directly, so object-count predicates
should reference the conventional count field, for example
`ncleaned_veto_Electron == 0`.

## Design principles

`fasthep-carpenter` focuses on:

* reusable analysis primitives
* declarative workflows
* registry-driven extension
* experiment-agnostic interfaces where possible
* compatibility with awkward-array based analysis ecosystems

The package intentionally separates workflow orchestration (`fasthep-flow`) from domain-specific analysis behaviour.

## Documentation

Main FAST-HEP documentation:

* [https://fast-hep.github.io](https://fast-hep.github.io)

API documentation for this package:

* [https://fasthep-carpenter.readthedocs.io/en/latest/](https://fasthep-carpenter.readthedocs.io/en/latest/)

## Repository

Main FAST-HEP repository and project links:

* [https://github.com/FAST-HEP/fasthep](https://github.com/FAST-HEP/fasthep)

## Contributing

Contribution guidelines, development setup, and project-wide documentation are maintained centrally in the main FAST-HEP repository.

## Legacy branch

The pre-split prototype implementation is preserved in the `legacy` branch.

The new `main` branch contains the split-package architecture.

## Status

FAST-HEP is currently in active pre-alpha development.

Interfaces may still evolve rapidly while the package split and stabilization work continues.
