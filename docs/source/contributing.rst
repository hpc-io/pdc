.. _contributing:

**6.** Contributing
===================

This page contains the standards for the PDC projects, covering coding, repository management, pull requests, documentation, and tests.

**6.1.** Coding Standards
-------------------------

- PDC uses the ``clang-format`` check for all ``.c``, ``.h``, ``.cpp``, ``.hpp`` files.
- PDC CI will ensure the code follows that format and push any changes that can be automatically fixed during this process.
- Novel features must be accompanied by their respective tests:
  
  - Unit tests
  - Functionality and integration tests
  - Examples
  - Check if any of the new tests should be run as part of the CI
  - Specification for the CI checklist integration
  - Should be looked at in the review process
  - Include as a checklist on the PR template

- Novel features must be accompanied by their respective in-code documentation:
  
  - Include as a checklist on the PR template
  - **L1 (mandatory)**: function, short summary, params, return values (exceptions), TODO/future improvements
  - **L2 (if needed)**: algorithm description, data structure description, complex code, optimizations
  - Should be looked at in the review process

**6.2.** Repository
-------------------

Branches Policy
~~~~~~~~~~~~~~~

- PDC officially has two main branches: ``stable`` and ``develop``.
- To include your contributions in PDC, a few scenarios might arise:

  - Everything has to go through an issue, and issue approval will determine if it goes into a fork or main repository (branch) (internal)
  - Branch creation in the main repository should be discussed and approved
  - If changes are in a cloned repository, after a while, if relevant, discuss moving to main repository in a specific branch

- **Approved Overall process** (not the main repository):

  1. Create your own clone; **do NOT** create additional branches in the main repository for this
  2. Create a new branch based on the ``develop`` branch
  3. Make your changes
  4. Open a pull request to the official PDC repository into the ``develop`` branch

- **All PRs must**:

  - Have a clear description of the changes
  - Have all discussions resolved
  - Pass all tests in the public CI
  - Branch should be updated with ``develop``
  - Pass the NERSC CI test, after approval from a repository maintainer

  .. note::
     Whoever starts/approves this CI to start is responsible for checking the code to ensure it does not include any malicious code, especially from first-time contributors.

  - Include documentation changes (if needed)
  - Include tests (for new features)

- Everything must first be merged into ``develop`` branch, and once a new release is made, that branch is synchronized with the ``stable`` one.
- Release train, plan for the monthly meeting for this.

**PR merge process approval:**

- Two reviewers for ``develop``
- Two reviewers for ``stable``
- No bypass of the merging rules
- Squash and Merge by default; exceptions may be requested
- For PRs in ``develop`` branch, at least one approval is required to merge
- For PRs in the ``stable`` branch, two approvals are required to merge

Labels Policy
~~~~~~~~~~~~~

- PDC has a list of labels to be applied to issues and PRs:  
  https://github.com/hpc-io/pdc/labels

-  Update labels:

  - Include type prefix for: ``bug``, ``CI``, ``documentation``, ``enhancement``, ``new feature``, ``question``, ``tests``
  - Include decision prefix for: ``duplicate``, ``help wanted``, ``invalid``, ``wontfix``

- **Issue Title Format**

  .. code-block:: text

     [Proposed Due Date][Priority][Type][Composer ID][Title][Related PR#]

     Example:
     [2023/06/23][Medium][DOC][PR#49][wzhang] Dart Integration - missing documentation
     [2023/06/23][Low][DOC][N/A][wzhang] Periodical Format Check

- Most go to labels, others such as PR and Issues references should be disclosed by reference.
- Before working on an issue, make sure it has the appropriate labels.
- Every change should start with an issue.
- When opening a PR and reviewing one, make sure labels are there and correctly reflect the content.

Pull Request Template
---------------------

.. code-block:: md

   # Related Issues / Pull Requests

   List all related issues and/or pull requests if there are any.

   # Description

   Include a brief summary of the proposed changes.

   # What changes are proposed in this pull request?

   - [ ] Bug fix
   - [ ] New feature
   - [ ] Breaking change
   - [ ] Documentation update

   # Checklist:

   - [ ] My code modifies existing public API, or introduces new public API, and I updated or wrote docstrings
   - [ ] I have commented my code
   - [ ] My code requires documentation updates, and I have made corresponding changes
   - [ ] I have added tests
   - [ ] All unit tests pass locally with my changes

Issues
------

- PDC has four templates to help create issues. Users can still open blank issues.

**Bug Report template:**

.. code-block:: md

   **Bug Report**
   A clear and concise description of what the bug is.

   **To Reproduce**
   How are you building/running PDC?

   ```bash
   ...