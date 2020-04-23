Golang Github Actions
~~~~~~~~~~~~~~~~~

Provides a two github actions workflows.

**To Enable:**

Add ``lyft/github_workflows`` to your ``boilerplate/update.cfg`` file.

Create a secret in github -> repo settings named ``flytegithub_dockerhub`` and put ``flytegithub`` dockerhub user's password.

The actions will push to 4 repos:

	1. ``docker.io/lyft/<repo>``
	2. ``docker.io/lyft/<repo>-stages`` : this repo is used to cache build stages to speed up iterative builds after.
	3. ``docker.pkg.github.com/lyft/<repo>/operator``
	5. ``docker.pkg.github.com/lyft/<repo>/operator-stages`` : this repo is used to cache build stages to speed up iterative builds after.
