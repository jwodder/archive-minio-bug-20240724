import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["run"]  # default session


@nox.session
def run(session):
    session.install("dandi")
    session.run("python", "mvce.py", *session.posargs)
