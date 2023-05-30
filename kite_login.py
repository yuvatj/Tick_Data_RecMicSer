# Standard Python library imports
import os
import sys
import json
import time

# Pyotp
from pyotp import TOTP

# Kite Connect
from kiteconnect import KiteConnect

# Selenium
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

# Local library imports
import models
from utility import Utility


# Parameters
ut = Utility()
# logging.basicConfig(level=logging.DEBUG)


class LoginCredentials:
    """ to create access token and other details """

    def __init__(self):
        self.date = ut.today
        self.credentials = self.__gen_credentials()
        self.api_key = self.credentials.api_key
        self.api_secret = self.credentials.api_secret
        self.user_id = self.credentials.kite_username
        self.password = self.credentials.kite_password
        self.totp = TOTP(self.credentials.totp_secret).now()

        # Change kwargs "auto=True to auto=False" for manual "access-token" generation
        self.credentials.access_token = self.__gen_access_token(auto=True)  # (auto=False)

    @staticmethod
    def __gen_credentials():
        """ to create api_key, api_secret, totp_secret """
        log_file = None
        file = 'login_credentials.json'

        while log_file is None:

            try:
                # try to access the existing file
                with open(file, "r") as f:
                    log_file = json.load(f)

            except FileNotFoundError:
                # Will create a file with user input
                print("---- Enter you Login Credentials ----")

                log_credential = {
                    "api_key": str(input("Enter API key :")),
                    "api_secret": str(input("Enter API Secret :")),
                    "totp_secret": str(input("Enter TOTP Secret :")),
                    "kite_username": str(input('Enter kite user name :')),
                    "kite_password": str(input('Enter kite password :')),
                    "mysql_username": str(input('Enter mysql user name :')),
                    "mysql_password": str(input('Enter mysql password :'))
                }

                # Give an option to save the data entered by the user
                user_decision = input("Press Y to save login credential and press any key to bypass : ").upper()

                if user_decision == "Y":
                    with open(file, "w") as data:
                        json.dump(log_credential, data)
                    print("Data Saved...")

                elif user_decision == 'EXIT':
                    print("Session canceled!!!!!")
                    sys.exit()

                else:
                    print("Data Save canceled!!!!!")

        return models.CredentialsModel(**log_file)

    def __gen_access_token(self, auto=True):
        """ to create access token """

        request_token = None
        access_token = None
        kite = KiteConnect(api_key=self.api_key)
        file_path = f"AccessToken/{self.date}.json"
        folder = "AccessToken"

        if os.path.exists(file_path):
            with open(file_path, 'r') as acc_data:
                access_token = json.load(acc_data)
                return access_token

        elif auto:

            # Initiate auto login with selenium
            url = kite.login_url()
            options = webdriver.EdgeOptions()
            options.add_argument('--headless')
            # options.add_argument('--disable-gpu')
            driver_path = "msedgedriver.exe"
            serv_obj = Service(driver_path)
            driver = webdriver.Edge(service=serv_obj, options=options)
            driver.get(url)
            time.sleep(4)
            # Enter user-name and password
            username = driver.find_element(By.ID, value='userid')
            password = driver.find_element(By.ID, value='password')
            username.send_keys(self.user_id)
            password.send_keys(self.password)
            driver.find_element(By.CLASS_NAME, value='actions').click()
            time.sleep(3)
            # Generate 'T-otp' and enter in the form
            otp = driver.find_element(By.XPATH, value='//*[@id="container"]/div[2]/div/div/form/div[1]/input')
            totp = self.totp
            otp.send_keys(totp)
            driver.find_element(By.CLASS_NAME, value='actions').click()
            time.sleep(3)
            # Extract request token from url
            request_token = driver.current_url.split('request_token=')[1][:32]
            driver.quit()

        else:
            print("---Getting Access Token manually---")
            print("Trying log In...")
            print("Login url : ", kite.login_url())
            request_token = input("Login and enter your 'request token' here : ")
            access_token = kite.generate_session(request_token=request_token,
                                                 api_secret=self.credentials.api_secret)["access_token"]

        try:
            access_token = kite.generate_session(request_token=request_token,
                                                 api_secret=self.credentials.api_secret)["access_token"]
            os.makedirs(folder, exist_ok=True)
            ut.empty_the_folder(folder)
            with open(file_path, "w") as f:
                json.dump(access_token, f)
            print("Login successful...")

        except Exception as e:
            print(f"Login Failed {e}")

        return access_token


if __name__ == "__main__":
    log = LoginCredentials()
    print(log.credentials)

