#!/usr/bin/env python3
#
# Copyright 2023 RelationalAI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

from setuptools import setup, find_packages

import workflow

setup(
    author="RelationalAI, Inc.",
    author_email="support@relational.ai",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.9",
    ],
    description="The RelationalAI Workflow Manager for batch runs",
    install_requires=[
        "tomli==2.0.1",
        "ed25519==1.5",
        "rai-sdk==0.6.14",
        "protobuf==3.20.2",
        "pyarrow==6.0.1",
        "requests==2.31.0",
        "requests-toolbelt==1.0.0",
        "more-itertools==10.1.0",
        "urllib3==1.26.6",
        "azure-storage-blob==12.17.0"],
    license="http://www.apache.org/licenses/LICENSE-2.0",
    long_description="The RAI Workflow Framework, which allows you to execute batch configurations, along with a "
                     "default Command-Line Interface (CLI) implementation to interact with the RAI Workflow "
                     "Framework. With this powerful combination, you can easily manage and execute batch "
                     "configurations for various tasks using the RAI database.",
    long_description_content_type="text/markdown",
    name="rai-workflow-manager",
    packages=find_packages(exclude=['test', 'cli-e2e-test']),
    include_package_data=True,
    url="https://github.com/RelationalAI/rai-workflow-manager",
    version=workflow.__version__)
