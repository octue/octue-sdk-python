Updating an Octue service
=========================

This page describes how to update an existing, deployed Octue service - in other words, how to deploy a new Octue
service revision.

We assume that:

- Your service's repository is on GitHub and you have push access to it
- Octue's `standard deployment GitHub Actions workflow <https://github.com/octue/workflows/blob/main/.github/workflows/deploy-cloud-run-service.yml>`_
  is set up in the repository and being used to deploy the service to Google Cloud Run on merge of a pull request into
  the ``main`` branch (see an example `here <https://github.com/octue/example-service-cloud-run/blob/main/.github/workflows/cd.yaml>`_)
- A release workflow is set up that will tag and release the new service revision on GitHub (see an example
  `here <https://github.com/octue/example-service-cloud-run/blob/main/.github/workflows/release.yml>`_)

Instructions
-------------

1. Check out and pull the ``main`` branch to make sure you're up to date with the latest changes

   .. code-block:: shell

       git checkout main
       git pull

2. If not already installed, install your service locally so you can run the tests and your development environment can
   lint the code etc:

   .. code-block:: shell

       poetry install

3. If using ``pre-commit`` to enforce code quality, make sure it's set up properly (you only need to do this once per
   repository, not on every update)

   .. code-block:: shell

       pre-commit install && pre-commit install -t commit-msg

4. Check out a new branch so you can work independently of any other work on the code happening at the same time

   .. code-block:: shell

       git checkout -b my-new-feature

5. Add and make changes to your app's code as needed, committing each self-contained change

   .. code-block:: shell

       git add changed-file.py another-changed-file.py
       git commit
       ...repeat...

   Push your commits frequently so your work is backed up on GitHub

   .. code-block:: shell

       git push

6. Write any new tests you need to verify your code works and update any old tests as needed

7. Run the tests locally using e.g. ``pytest`` or ``python -m unittest`` and fix anything that makes them fail

8. Update the semantic version of your app:

   - ``poetry version patch`` for a bug fix or small non-code change
   - ``poetry version minor`` for a new feature
   - ``poetry version major`` for a breaking change

   Don't forget to commit this change, too.

9. When you're ready to review the changes, head to GitHub and open a pull request of your branch into ``main``. This
   makes it easy for you and anyone else to see what's changed. Check the diff to make sure everything's there and
   consistent (it's easy to forget to push a commit). Ask your colleagues to review the code if required.

10. When you're ready to release the new version of your service, check that the GitHub checks have passed on the latest
    commit. The checks ensure code quality, the tests pass, and that the new version of the code is correct.

11. Merge the pull request into ``main``. This will run the deployment workflow (usually called ``cd`` - continuous
   deployment), making the new version of the service available to everyone.

12. Check that the deployment workflow has run successfully (this can take a few minutes). You can check the progress in
    the "Actions" tab of the GitHub repository
