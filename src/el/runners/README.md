# runners/

This folder contains local test scripts for el_jobs classes.

Each file here allows individual testing of an el_job (by endpoint)
without going through Airflow. This makes it easier to develop, debug, and validate
the process.


> The .py files are ignored by the Git repository via .gitignore to avoid cluttering
the repository with temporary or non-production-executable code.
