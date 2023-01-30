# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import jsii
import pytest
from aws_cdk.aws_lambda import (
    Code,
    FunctionProps,
    Runtime,
    Function,
    LayerVersionProps,
    LayerVersion,
)
from constructs import Construct


shared_path = str(Path(__file__).parent.parent)
if shared_path not in sys.path:
    sys.path.insert(0, shared_path)


def mock_lambda_init(
    self,  # NOSONAR (python:S107) - allow large number of method parameters
    scope: Construct,
    id: str,
    *,
    code: Code,
    handler: str,
    runtime: Runtime,
    **kwargs,
) -> None:
    # overriding the code will prevent building lambda functions
    # override the runtime list for now, as well, to match above
    props = FunctionProps(
        code=Code.from_inline("return"),
        handler=handler,
        runtime=Runtime.PYTHON_3_7,
        **kwargs,
    )
    jsii.create(Function, self, [scope, id, props])


def mock_layer_init(self, scope: Construct, id: str, *, code: Code, **kwargs) -> None:
    # overriding the layers will prevent building lambda layers
    # override the runtime list for now, as well, to match above
    with TemporaryDirectory() as tmpdirname:
        kwargs["code"] = Code.from_asset(path=tmpdirname)
        kwargs["compatible_runtimes"] = [Runtime.PYTHON_3_7]
        props = LayerVersionProps(**kwargs)
        jsii.create(LayerVersion, self, [scope, id, props])


@pytest.fixture(autouse=True)
def cdk_lambda_mocks(mocker, request):
    """Using this session mocker means we cannot assert anything about functions or layer versions of this stack"""
    if "no_cdk_lambda_mock" in request.keywords:
        yield
    else:
        mocker.patch("aws_cdk.aws_lambda.Function.__init__", mock_lambda_init)
        # mocker.patch("aws_cdk.aws_lambda.LayerVersion.__init__", mock_layer_init)
        yield


@pytest.fixture(autouse=True)
def cdk_entrypoint():
    """This otherwise would not be importable (it's not in a package, and is intended to be a script)"""
    sys.path.append(str((Path(__file__).parent.parent / "infrastructure").absolute()))
    yield


@pytest.fixture
def cdk_json():
    path = Path(__file__).parent.parent / "infrastructure" / "cdk.json"
    return json.loads(path.read_text())
