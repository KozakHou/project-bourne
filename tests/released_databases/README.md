# Released database fixtures

These SQLite databases were created by installing the exact public PyPI wheels
`bourneprov==0.1.1` and `bourneprov==0.2.0` in clean temporary virtual
environments and running their published `bourne` console entry points.

- `bourneprov-0.1.1.db` contains completed, failed, and interrupted experiments.
- `bourneprov-0.2.0.db` contains completed and failed experiments, present and
  missing artifacts, execution-context data, and `derived_from` lineage.

Machine-specific hostname and private home-prefix strings were replaced with
`fixture-host` and `/opt/test/conda`, followed by `VACUUM`. The released schemas,
row relationships, statuses, artifact states, and provenance shapes were not
reconstructed by current source code.
