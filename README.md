[![CI](https://github.com/nogibjj/706_Week01_YL/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/706_Week01_YL/actions/workflows/cicd.yml)

# 706_Week01_YL

This repository includes the main tasks for Week 1:

* `Makefile` is a configuration file used in Unix-based systems for automating tasks and building software. It contains instructions and dependencies for compiling code, running tests, and other development tasks.
* `.devcontainer` includes a Dockerfile and `devcontainer.json`. The `Dockerfile` within this folder specifies how the container should be built, and other settings in this directory may control development environment configurations.
* `Workflows` includes GitHub Actions, which contain configuration files for setting up automated build, test, and deployment pipelines for your project.
* `.gitignore` is used to specify which files or directories should be excluded from version control when using Git.
* `README.md` is the instruction file for the readers.
* `descriptive.py` is a Python file that contains the main function.
* `test_descriptive.py`  is a test file for `descriptive.py` that can successfully run in IDEs.
* `requirements.txt` is to specify the dependencies (libraries and packages) required to run the project.

## Project description


## Project environment

* Use codespace for scripting
* Container built in `devcontainers` and virtual environment activated via `requirements.txt`
* To run the code, use the command `python main.py` in the terminal

## Check format & errors

1. make format

2. make lint

3. make test