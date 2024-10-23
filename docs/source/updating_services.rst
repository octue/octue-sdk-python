Updating an Octue service
=========================

This page describes how to update an existing, deployed Octue service - in other words, how to deploy a new Octue service revision.

We assume that:

- Your service's repository is on GitHub and you have push access to it
- Octue's `standard deployment GitHub Actions workflow <https://github.com/octue/workflows/blob/main/.github/workflows/deploy-cloud-run-service.yml>`_ is set up in the repository and being used to deploy the service to Google Cloud Run on merge of a pull request into the ``main`` branch (see an example `here <https://github.com/octue/example-service-cloud-run/blob/main/.github/workflows/cd.yaml>`_)
- A release workflow is set up that will tag and release the new service revision on GitHub (see an example `here <https://github.com/octue/example-service-cloud-run/blob/main/.github/workflows/release.yml>`_)

Instructions
-------------

1. Check out and pull the ``main`` branch

   .. code-block:: shell
       git checkout main
       git pull

2. If not already installed, install your service locally.

   If using ``poetry``:

   .. code-block:: shell
       poetry install

   If using ``pip``:

   .. code-block:: shell
       pip install -e .

3. If using ``pre-commit``, make sure it's set up properly (you only need to do this once per repository, not on every update)

   .. code-block:: shell
       pre-commit install && pre-commit install -t commit-msg

4. Check out a new branch

   .. code-block:: shell
       git checkout -b my-new-feature

5. Add and make changes to your app's code as needed, committing each self-contained change to git

   .. code-block:: shell
       git add changed-file.py another-changed-file.py
       git commit
       ... repeat ...

   Push your commits frequently so your work is backed up on your git provider (e.g. GitHub)

   .. code-block:: shell
       git push

6. Write any new tests you need to verify your code works and update any old tests as needed

7. Run the tests locally using e.g. ``pytest`` or ``python -m unittest``

8. Update the semantic version of your app. If you're using ``poetry``, run:

   - ``poetry version patch`` for a bug fix or small non-code change
   - ``poetry version minor`` for a new feature
   - ``poetry version major`` for a breaking change

   If you're using ``pip``, manually update the version in ``setup.py`` or ``pyproject.toml``. Don't forget to commit this change, too.

9. When you're ready to review the changes, head to GitHub and open a pull request of your branch into ``main``. Check the diff to make sure everything's there and consistent (it's easy to forget to push everything). Ask your colleagues to review the code if required.

10. When you're ready to release the new version of your service, check that the GitHub checks have passed on the latest commit

11. Merge the pull request

12. Check that the deployment workflow (usually called ``cd`` or ``ci``) has run successfully (this can take a few minutes). You can check the progress in the "Actions" tab of the GitHub repository

13. Run a deployment test for the new service revision if you have one
