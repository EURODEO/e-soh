# e-soh-datastore-poc

E-SOH datastore PoCs

## Pre-commit

### Setup

1. Go to the root of the repository.
2. Install the python pre-commit package with `pip install pre-commit`.
3. Reinitialize the repository with `git init`.
4. Install the hooks defined in `.pre-commit-config.yaml` with `pre-commit install`.

### Useful Commands

- To update the pre-commit hooks in `.pre-commit-config.yaml`, run `pre-commit autoupdate`
- To apply the pre-commit for every file in the repository, run `pre-commit run --config './.pre-commit-config.yaml' --all-files`
- To see all options to `pre-commit run`, run `pre-commit help run` (in particular, the `--files` option can be used to apply the command to selected files only).
- To commit without the pre-commit hook, run `git commit -m "Some message" --no-verify`
