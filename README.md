# RAI Workflow Framework

This repository contains the RAI Workflow Framework, which allows you to execute batch configurations, along with a default Command-Line Interface (CLI) implementation to interact with the RAI Workflow Framework. With this powerful combination, you can easily manage and execute batch configurations for various tasks using the RAI database.

## Project Status
[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

⚠️ **Warning: This project is under active development.**

Please note that this project is in the early stages of development, and as such, it may undergo significant changes in subsequent versions. We are actively working on improving and expanding the functionality of the software.
### Compatibility Notice

As the project evolves, there may be changes that could affect compatibility with earlier versions. It is advisable to check the release notes and update your code accordingly when upgrading to a new version.

## Getting Started

### Introduction

The RAI Workflow Framework is designed to streamline complex task execution by providing a comprehensive solution for managing batch configurations. This framework abstracts workflow execution, enabling you to focus on defining tasks (workflow steps) and configurations for your projects.

The included CLI implementation enhances your experience by providing an intuitive interface to interact with the RAI Workflow Framework. You can execute batch configurations effortlessly, without needing to dive into the technical details.

### Requirements

* Python 3.9+

### Build project

```bash
pyenv install 3.9
pyenv local 3.9
pip install --upgrade pip
pip install virtualenv
python -m virtualenv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

### RAI Workflow Framework

For detailed information about the RAI Workflow Framework, including its architecture, usage instructions, and examples, please refer to the [RAI Workflow Framework README](workflow/README.md).

### CLI Implementation

To learn more about the CLI implementation, its features, installation guide, and usage examples, please visit the [CLI README](cli/README.md).

## Support

This software is released as-is. RelationalAI provides no warranty and no support on this software. If you have any issues with the software, please file an issue. 

## License

The RAI Workflow Manager is licensed under the
Apache License 2.0. See:
https://github.com/RelationalAI/rai-workflow-manager/blob/main/LICENSE