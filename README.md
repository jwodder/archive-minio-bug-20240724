This is an MVCE for a bug in [Dandi Archive][] and/or minio discovered while
migrating [`backups2datalad`][]'s tests from using the staging Archive to using
a Dockerized Archive.

Specifically, when using a versioned minio container, if an Archive client
uploads an entry within a Zarr, deletes the entry, and then uploads a new entry
whose path is beneath the first entry's path (effectively changing the first
entry's path from a file to a directory), the second upload will have no effect
and the second entry will not be stored.  If versioning is not used (e.g., by
deleting the `docker compose run --rm createbuckets` command from `mvce.py`),
then the second upload will succeed.

The MVCE can be run by running [`nox`](https://nox.thea.codes) in a clone of
this repository.  If invoked as `nox -- --quiet`, output from Docker Compose
commands will be suppressed.

[Dandi Archive]: https://github.com/dandi/dandi-archive
[`backups2datalad`]: https://github.com/dandi/backups2datalad
