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
This module contains helper functions for obtaining
an access token directly from SFMC
"""
import requests


class AccessTokenException(Exception):
    """
    This class is a subclass of Exception for access token errors
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class AccessToken:
    """
    This class encapsulates the HTTP layer
    for retrieving an access token
    """

    def __init__(self, client_id, client_secret, token_endpoint) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_endpoint = token_endpoint
        self.grant_type = "client_credentials"
        self.body = {
            "grant_type": self.grant_type,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    def retrieve_token(self):
        """
        This function is responsible for retrieving a
        token over HTTP from an OIDC provider
        """
        response = requests.post(self.token_endpoint, json=self.body)
        if response.ok:
            json = response.json()
            return {
                "access_token": json["access_token"],
                "expires_in": json["expires_in"],
            }
        else:
            raise AccessTokenException(
                f"{response.status_code} status returned from {self.token_endpoint}"
            )
