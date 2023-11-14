# Standard Python library imports
import json
import time
import datetime

# Pyotp
from pyotp import TOTP

# Kite Connect
from kiteconnect import KiteConnect

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Local library imports
# __none__


class BaseClass:
    """ Base class for other operations """

    def __init__(self, website: str, user_input: str):

        self.website = website
        self.user_name = user_input
        self.date = datetime.date.today()
        self.file_name = "credentials.json"  # "credentials.json"
        self.folder = "E:/Market Analysis/Programs/Deployed/utility/User_Login/"

        self.user_data = None
        self.user_data_file = None

        self.__load_or_create_config()
        self.__load_or_create_user()

    def __get_user_structure(self):

        structure_mapping = {
            "kite": ("website", "username", "password", "api_key", "api_secret", "totp_secret", "access_token", "last_update"),
            "smart_api": ("website", "username", "password", "api_key", "api_secret", "totp_secret")
        }

        start = True
        structure = {}
        print(f"\nTrying to create a new user for {self.user_name}:")

        while start:

            for attribute in structure_mapping[self.website]:

                if attribute == 'website':
                    value = self.website
                elif attribute == 'username':
                    value = self.user_name
                elif attribute == 'last_update':
                    value = str(self.date)
                else:
                    value = input(f"Enter {attribute}: ")
                structure[attribute] = value

            start = False if input("\nEnter 'yes' to save user: ").lower() == 'yes' else True

        return structure

    def __load_or_create_config(self):

        path = self.folder + self.file_name

        try:
            with open(path, "r") as data:
                config = json.load(data)

            print("Config file loaded successfully.")

            self.user_data_file = config

        except FileNotFoundError:
            print(f"File not found at '{path}'.\nCreating a new configuration file.\n")

            # Get user input for MySQL credentials
            mysql_username = input("Enter MySQL username: ")
            mysql_password = input("Enter MySQL password: ")

            # Create a new configuration
            config = {
                "mysql": {"username": mysql_username, "password": mysql_password},
                "user": {}
            }

            # Save the new configuration to the file
            with open(path, 'w') as file:
                json.dump(config, file)

            print(f"Configuration data saved to '{self.file_name}'.")
            self.user_data_file = config

        except Exception as e:
            print(f"An error occurred while opening {self.file_name}: {e}")

    def __load_or_create_user(self):

        json_file = self.user_data_file

        if json_file is None:
            raise "user data file is empty"

        try:
            user_data = json_file['user']
            user = user_data[self.user_name]
            self.user_data = user

        except KeyError:
            print(f"User not found: {self.user_name}.")

            user_input = input(f"Do you want to create a new user for {self.user_name}? yes/no: ")

            if user_input.lower() == 'yes':

                new_user_id = self.user_name
                json_file = self.user_data_file
                path = self.folder + self.file_name
                new_user_data = self.__get_user_structure()

                json_file['user'][new_user_id] = new_user_data

                final_data = json_file
                # Write the updated data back to the file
                with open(path, 'w') as file:
                    json.dump(final_data, file)

                self.user_data = new_user_data

            else:
                return None

        except Exception as e:
            print(f"An error occurred while getting user {self.user_name}: {e}")


class KiteUser(BaseClass):

    def __init__(self, user_name: str):
        super().__init__(website="kite", user_input=user_name)

        self.api_key = None
        self.api_secret = None
        self.access_token = None
        self.mysql_username = None
        self.mysql_password = None

        self.__fetch_access_token()  # order is important
        self.__fetch_credentials()

    def gen_totp(self):

        # Example secret (replace with your actual secret)
        secret = self.user_data['totp_secret']

        # Create a TOTP object
        totp = TOTP(secret)

        # Get the current time
        current_time = int(time.time())

        # Get the next TOTP value and its expiration time
        next_totp = totp.at(current_time)
        expiration_time = current_time + totp.interval - (current_time % totp.interval)

        # Calculate the time remaining until the next TOTP value expires
        time_remaining = expiration_time - current_time

        results = {
            "totp": next_totp,
            "time_remaining": time_remaining
        }

        return results

    def __auto_login(self):
        api_key = self.user_data['api_key']
        api_secret = self.user_data['api_secret']
        user_id = self.user_data['username']
        user_password = self.user_data['password']

        kite = KiteConnect(api_key=api_key)
        url = kite.login_url()

        options = webdriver.EdgeOptions()
        # options.add_argument('--headless')
        # options.add_argument('--disable-gpu')

        with webdriver.Edge(service=Service("msedgedriver.exe"), options=options) as driver:
            driver.get(url)

            # Fill login credentials
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'userid')))
            username = driver.find_element(By.ID, 'userid')
            password = driver.find_element(By.ID, 'password')
            username.send_keys(user_id)
            password.send_keys(user_password)
            driver.find_element(By.CLASS_NAME, 'actions').click()

            # Fill OTP
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="container"]/div[2]/div/div/form/div[1]/input')))
            otp = driver.find_element(By.XPATH, '//*[@id="container"]/div[2]/div/div/form/div[1]/input')
            totp = self.gen_totp()['totp']
            otp.send_keys(totp)
            driver.find_element(By.CLASS_NAME, 'actions').click()

            # Extract request token and generate access token
            WebDriverWait(driver, 10).until(EC.url_contains('request_token='))
            request_token = driver.current_url.split('request_token=')[1][:32]

            access_token = kite.generate_session(request_token=request_token, api_secret=api_secret)["access_token"]

        return access_token

    def __manual_login(self):

        api_key = self.user_data['api_key']
        api_secret = self.user_data['api_secret']

        user_id = self.user_data['username']
        user_password = self.user_data['password']

        kite = KiteConnect(api_key=api_key)

        print("---Getting Access Token manually---\n")
        print("Login url : ", kite.login_url())
        print(f"click on the above link a enter the username: {user_id}, password: {user_password}\n")

        user_input = input("Enter yes to flash the totp: ")
        if user_input.lower() == 'yes':
            for i in range(50):
                print(self.gen_totp())
                time.sleep(1)

        request_token = input("Login and enter your 'request token' here: ")

        access_token = kite.generate_session(request_token=request_token, api_secret=api_secret)["access_token"]

        return access_token

    def __fetch_access_token(self):

        last_update = self.user_data["last_update"]

        # Convert the string to a date object
        date_object = datetime.datetime.strptime(last_update, "%Y-%m-%d").date()

        if date_object < self.date:
            access_token = None
            try:
                # Get the current time
                current_time = time.localtime()

                # Calculate the time remaining until the next minute
                time_remaining = 60 - current_time.tm_sec

                print(f"Waiting for {time_remaining} sec")

                # Wait for the new minute to start
                time.sleep(time_remaining)

                access_token = self.__auto_login()

            except Exception as e:
                print(e)
                print(f"Looks like we are facing an issue with 'auto_login' process\n")

                choice = input("Do you want to rerun auto login? (yes/no): ").lower()

                if choice == "yes":

                    # Get the current time
                    current_time = time.localtime()
                    # Calculate the time remaining until the next minute
                    time_remaining = 60 - current_time.tm_sec
                    print(f"Waiting for {time_remaining} sec")
                    # Wait for the new minute to start
                    time.sleep(time_remaining)

                    access_token = self.__auto_login()

                else:
                    access_token = self.__manual_login()

            finally:

                if access_token is not None:

                    new_date = self.date
                    json_file = self.user_data_file
                    new_access_token = access_token

                    path = self.folder + self.file_name

                    json_file['user'][self.user_name]["last_update"] = str(new_date)
                    json_file['user'][self.user_name]["access_token"] = new_access_token

                    self.user_data_file = json_file
                    self.user_data = json_file['user'][self.user_name]

                    final_data = json_file

                    # Write the updated data back to the file
                    with open(path, 'w') as file:
                        json.dump(final_data, file)

    def __fetch_credentials(self):

        user_data = self.user_data
        mysql_data = self.user_data_file['mysql']

        self.api_key = user_data['api_key']
        self.api_secret = user_data['api_secret']
        self.access_token = user_data['access_token']

        self.mysql_username = mysql_data['username']
        self.mysql_password = mysql_data['password']


class LoginCredentials:
    """ To create access token and other details """

    def __init__(self):
        self.user = KiteUser("WV0236")  # Define user as an instance attribute

        # Access properties using self.user instead of user directly
        self.api_key = self.user.api_key
        self.api_secret = self.user.api_secret
        self.access_token = self.user.access_token

        self.mysql_username = self.user.mysql_username
        self.mysql_password = self.user.mysql_password


if __name__ == "__main__":
    # log = LoginCredentials()
    # print(log.credentials)

    # user = BaseClass('kite', "WV0236")
    user = KiteUser("WV0236")
    print(user.access_token)


