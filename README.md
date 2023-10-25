# e-soh-datastore-poc
E-SOH datastore PoCs


## Pre-commit
### Setup
1. Go to the root of the repository.
2. Install the python pre-commit package with `pip install pre-commit`.
3. Reinitialize there repository with `git init`.
4. Install the hooks defined in `.pre-commit-config.yaml` with `pre-commit install`.

### Useful Commands
- To update the pre-commit hooks in `.pre-commit-config.yaml` run: `pre-commit autoupdate`
- To run the pre-commit for every file in the repository run `pre-commit run --config './.pre-commit-config.yaml' --all-files`
- To commit without the pre-commit hook `git commit -m "Some message" --no-verify`
