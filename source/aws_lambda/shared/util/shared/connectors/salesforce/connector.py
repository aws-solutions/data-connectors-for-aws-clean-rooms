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
"""
This module contains helper functions for working with AppFlow connector profiles
"""

from aws_solutions.core.helpers import get_service_client

from shared.connectors.salesforce.token import AccessToken


class SalesforceConnectorProfile:
    """
    This class encapsulates the CUD functions for AppFlow connector profiles
    """

    def __init__(
        self,
        profile_name,
        client_id=None,
        client_secret=None,
        token_endpoint=None,
        instance_url=None,
    ):
        # only delete function can be used if profile_name is provided alone
        self.profile_name = profile_name
        self.connector_type = "CustomConnector"
        self.connector_mode = "Public"
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_endpoint = token_endpoint
        self.grant_type = "CLIENT_CREDENTIALS"
        self.instance_url = instance_url
        if client_id and client_secret and token_endpoint:
            self.access_token = AccessToken(
                self.client_id, self.client_secret, self.token_endpoint
            )

    def create(self):
        """
        This function is responsible for creating a new AppFlow connector profile
        """
        # retrieve a fresh access token
        token_data = self.access_token.retrieve_token()
        # create a new connection profile with the access token
        appflow_client = get_service_client("appflow")
        response = appflow_client.create_connector_profile(
            connectorProfileName=self.profile_name,
            connectorLabel="SalesforceMarketingCloud",
            connectionMode=self.connector_mode,
            connectorType="CustomConnector",
            connectorProfileConfig={
                "connectorProfileProperties": {
                    "CustomConnector": {
                        "profileProperties": {"instanceUrl": self.instance_url},
                        "oAuth2Properties": {
                            "tokenUrl": self.token_endpoint,
                            "oAuth2GrantType": self.grant_type,
                        },
                    }
                },
                "connectorProfileCredentials": {
                    "CustomConnector": {
                        "authenticationType": "OAUTH2",
                        "oauth2": {
                            "clientId": self.client_id,
                            "clientSecret": self.client_secret,
                            "accessToken": token_data["access_token"],
                        },
                    }
                },
            },
        )
        return response["connectorProfileArn"]

    def update(self):
        """
        This function is used to update the token used with a connector profile
        """
        # retrieve a fresh access token
        token_data = self.access_token.retrieve_token()
        # create an existing connection profile with the access token
        appflow_client = get_service_client("appflow")
        response = appflow_client.update_connector_profile(
            connectorProfileName=self.profile_name,
            connectionMode=self.connector_mode,
            connectorProfileConfig={
                "connectorProfileProperties": {
                    "CustomConnector": {
                        "profileProperties": {"instanceUrl": self.instance_url},
                        "oAuth2Properties": {
                            "tokenUrl": self.token_endpoint,
                            "oAuth2GrantType": self.grant_type,
                        },
                    }
                },
                "connectorProfileCredentials": {
                    "CustomConnector": {
                        "authenticationType": "OAUTH2",
                        "oauth2": {
                            "clientId": self.client_id,
                            "clientSecret": self.client_secret,
                            "accessToken": token_data["access_token"],
                        },
                    }
                },
            },
        )
        return response["connectorProfileArn"]

    def delete(self):
        """
        This function is responsible for removing an existing connector profile
        """
        appflow_client = get_service_client("appflow")
        appflow_client.delete_connector_profile(
            connectorProfileName=self.profile_name, forceDelete=False
        )
