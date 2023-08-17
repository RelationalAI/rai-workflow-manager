# RAI Workflow Framework

This repository contains the RAI Workflow Framework, which allows you to execute batch configurations, along with a default Command-Line Interface (CLI) implementation to interact with the RAI Workflow Framework. With this powerful combination, you can easily manage and execute batch configurations for various tasks using the RAI database.

## Introduction

The RAI Workflow Framework is designed to streamline complex task execution by providing a comprehensive solution for managing batch configurations. This framework abstracts workflow execution, enabling you to focus on defining tasks (workflow steps) and configurations for your projects.

The included CLI implementation enhances your experience by providing an intuitive interface to interact with the RAI Workflow Framework. You can execute batch configurations effortlessly, without needing to dive into the technical details.

## RAI Workflow Framework

For detailed information about the RAI Workflow Framework, including its architecture, usage instructions, and examples, please refer to the [RAI Workflow Framework README](workflow/README.md).

## CLI Implementation

To learn more about the CLI implementation, its features, installation guide, and usage examples, please visit the [CLI README](cli/README.md).

## Build project
```bash
pyenv install 3.9
pyenv local 3.9
pip install --upgrade pip
pip install virtualenv
python -m virtualenv venv
source ./venv/bin/activate
pip install -r requirements.txt
```
