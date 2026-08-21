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
   python -m pip install bourneprov==0.5.0
   bourne --version
   ```

The release workflow checks that the Git tag matches the package version,
builds one wheel and one source distribution, validates their metadata, runs the
full test suite against the installed wheel, publishes through OIDC, and
attaches the same distributions to the GitHub Release.
