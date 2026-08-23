# Releasing Project Bourne

Project Bourne publishes distributions through GitHub Actions and PyPI Trusted
Publishing. No long-lived PyPI API token is stored in GitHub.

## One-time configuration

Create a GitHub environment named `pypi`. Requiring approval from a trusted
maintainer before deployment is strongly recommended.

Configure the `bourneprov` Trusted Publisher on PyPI with these exact values:

```text
PyPI project:       bourneprov
GitHub owner:       KozakHou
Repository:         project-bourne
Workflow filename:  release.yml
Environment:        pypi
```

For the first publication, when the PyPI project does not exist yet, configure
these values as a pending publisher at:

<https://pypi.org/manage/account/publishing/>

For later releases, manage the publisher under the project's Publishing
settings on PyPI.

## License validation

Before publishing, confirm that:

- `pyproject.toml` contains the intended SPDX license expression;
- the root `LICENSE` and `NOTICE` files are present;
- wheel metadata contains the expected `License-Expression` and `License-File`;
- both the wheel and source distribution contain the intended license files.

## Locked development and build tooling

`uv.lock` is the committed authority for Python development and validation.
Before preparing a release, run:

```bash
uv sync --locked --all-extras --dev
uv run --frozen --no-sync python -W error::ResourceWarning -m unittest discover -s tests -v
uv build --no-sources
uv run --frozen --no-sync twine check dist/*
```

`uv build --no-sources` deliberately validates the standards-based package
metadata rather than any local uv source override. The setuptools build backend
remains authoritative. uv is not installed into the released package, remote
worker, HPC environment, or npm launcher.

## Publishing a release

1. Update and validate the version in `pyproject.toml`.
2. Ensure the full GitHub Actions CI matrix passes on `main`.
3. Draft a GitHub Release whose tag is exactly `v<version>`, such as `v0.5.0`,
   targeting the intended commit on `main`.
4. Publish the GitHub Release.
5. Approve the `pypi` environment deployment if environment protection requires
   it.
6. Verify that the Release workflow succeeds, the wheel and source distribution
   are attached to the GitHub Release, and the version appears on PyPI.
7. Verify installation in a fresh environment:

   ```bash
   python -m pip install bourneprov==0.6.0
   bourne --version
   ```

The release workflow checks that the Git tag matches the package version,
performs a locked uv sync, builds one wheel and one source distribution with
`uv build --no-sources`, validates their metadata, runs the full test suite
against the installed wheel, publishes through OIDC, and attaches the same
distributions to the GitHub Release.

## v0.6 multi-registry sequence

The existing GitHub Release workflow is the authority for Python publication,
so the final v0.6 order is:

1. Merge the reviewed v0.6 pull request and require the full Python and Node
   matrices to pass on `main`.
2. Prepare and review the final Python and npm versions and package contents.
3. Publish the `v0.6.0` GitHub Release from the intended `main` commit. Wait for
   its existing trusted-publishing workflow to publish and verify
   `bourneprov==0.6.0` on PyPI and attach the Python distributions.
4. Publish `@project-bourne/mcp@0.6.0` to npm only from a maintainer identity
   that controls the `@project-bourne` scope.
5. Verify the public npm tarball, exact Python-version coupling, and that its
   `mcpName` is `io.github.KozakHou/project-bourne`.
6. Publish the root `server.json` with the official `mcp-publisher`, then verify
   the official Registry API and search result.
7. Verify public GitHub Skill search against generic scientific-execution
   intents after the Skill is merged.

Never publish Registry metadata for an npm version that does not yet exist.
Do not run `gh skill publish` as part of v0.6 unless its interaction with the
existing `v0.6.0` tag and GitHub Release has been reviewed; the merged public
`SKILL.md` remains discoverable without a second release.

For future Registry automation, use a post-npm, non-pull-request GitHub Actions
job with `id-token: write`, `mcp-publisher login github-oidc`, and
`mcp-publisher publish`. Pin and verify the publisher tooling, require the final
npm package to exist first, and do not add a long-lived Registry secret. This
strategy is documented here but is intentionally not activated during the
v0.6 release-candidate preparation.

## GitHub discovery metadata

After merge, add this focused topic set as an explicit repository-maintenance
action (repository topics are not changed implicitly by the code pull request):

```text
scientific-computing
reproducibility
provenance
experiment-tracking
mcp
model-context-protocol
agent-skills
ai-agents
hpc
slurm
pbs
gpu
simulation
```
