import requests
import base64
from urllib.parse import urlencode


class MercadoLivreClient:
    def __init__(self, client_id, client_secret, redirect_uri):
        """
        Initialize Mercado Livre API client

        :param client_id: Application's Client ID
        :param client_secret: Application's secret key
        :param redirect_uri: Redirect URI registered in the application
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.access_token = None
        self.refresh_token = None

    def generate_authorization_url(self):
        """
        Generate the authorization URL for Mercado Livre OAuth

        :return: Authorization URL
        """
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
        }
        return f"https://auth.mercadolivre.com.br/authorization?{urlencode(params)}"

    def get_access_token(self, authorization_code):
        """
        Exchange authorization code for access token

        :param authorization_code: Code received after user authorization
        :return: Dictionary with token details
        """
        token_url = "https://api.mercadolibre.com/oauth/token"

        payload = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": authorization_code,
            "redirect_uri": self.redirect_uri,
        }

        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        }

        response = requests.post(token_url, data=payload, headers=headers)

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token")
            return token_data
        else:
            raise Exception(f"Token retrieval failed: {response.text}")

    def refresh_access_token(self):
        """
        Refresh the access token using the refresh token

        :return: Dictionary with new token details
        """
        if not self.refresh_token:
            raise ValueError("No refresh token available")

        token_url = "https://api.mercadolibre.com/oauth/token"

        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        }

        response = requests.post(token_url, data=payload, headers=headers)

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token", self.refresh_token)
            return token_data
        else:
            raise Exception(f"Token refresh failed: {response.text}")

    def get_user_info(self):
        """
        Retrieve user information using the access token

        :return: User information dictionary
        """
        if not self.access_token:
            raise ValueError("No access token available")

        user_url = "https://api.mercadolibre.com/users/me"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = requests.get(user_url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"User info retrieval failed: {response.text}")


def main():
    # Your specific credentials
    CLIENT_ID = "6013482548134050"
    CLIENT_SECRET = "mAAH2IHAOSvgeZQtYvSg0DrNogBCWfro"
    REDIRECT_URI = "https://www.noneca.com/"

    # Authorization code from the URL
    AUTHORIZATION_CODE = "TG-67e6b9bdf1c27500011b66c6-354140329"

    # Create client instance
    client = MercadoLivreClient(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)

    try:
        # Get access token
        token_response = client.get_access_token(AUTHORIZATION_CODE)
        print("Access Token Retrieved Successfully!")

        # Get user information
        user_info = client.get_user_info()
        print("\nUser Information:")
        print(f"User ID: {user_info.get('id')}")
        print(f"Nickname: {user_info.get('nickname')}")
        print(
            f"Name: {user_info.get('first_name', '')} {user_info.get('last_name', '')}"
        )
        print(f"Email: {user_info.get('email')}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
