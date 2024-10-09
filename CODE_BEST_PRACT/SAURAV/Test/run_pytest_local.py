# Databricks notebook source
import pytest
import sys
import os

# Get the current notebook path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Define the repository path for the branch feature_05-10-2024_Saurav
repo_root = f'/Workspace/users/saurav.chaudhary@celebaltech.com/Coding-Best-Practices/feature_05-10-2024_Saurav'

# Ensure pytest looks in the right directory by adjusting the sys.path
sys.path.insert(0, repo_root)

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest with verbosity (-v) and test matching pattern '-k .'
retcode = pytest.main([ '--tb=short', '--disable-warnings', '--maxfail=1',
    '-k', '.',
    '-v',
])

# Fail the cell execution if we have any test failures.
assert retcode == 0, 'The pytest invocation failed. See the log above for details.'

# COMMAND ----------


