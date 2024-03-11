import os
import sys
import zipfile
import logging

import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, Inserter
import configparser

# Create a local logger
logger = logging.getLogger(__name__)
f_handler = logging.FileHandler('tsc.log')
f_format = logging.Formatter('%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
                             datefmt="%Y/%m/%d %H:%M:%S")
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)
logger.setLevel(logging.INFO)

# Read configuration from config.ini file
config = configparser.ConfigParser()
config.read(r'D:\TableauUser\Programs\config.ini')  # Mega

# Tableau Server configuration
SERVER_URL = config.get('Tableau', 'SERVER_URL')
SITE_ID = config.get('Tableau', 'SITE_ID')
TOKEN_NAME = config.get('Tableau', 'TOKEN_NAME')
TOKEN_SECRET = config.get('Tableau', 'TOKEN_SECRET')
PAGESIZE = config.get('Tableau', 'PAGESIZE')

# File path
EXCEL_FILE = config.get('File', 'EXCEL_FILE')
HYPER_FILE = config.get('File', 'HYPER_FILE')

# Group and role information
GROUP_NAME = config.get('Group', 'GROUP_NAME')
GROUP_ID = config.get('Group', 'GROUP_ID')
VIEW_ROLE = config.get('Group', 'VIEW_ROLE')
UNLICENSED_ROLE = config.get('Group', 'UNLICENSED_ROLE')


class tableau_group_users:

    def __init__(self, group_name, server_url, token_name, token_secret, site_id, hyper_file=None):
        self.server = TSC.Server('http://{}'.format(SERVER_URL), use_server_version=True)  # Mega S
        self._group_name = group_name
        self._server_url = server_url
        self._token_name = token_name
        self._token_secret = token_secret
        self._site_id = site_id
        self._hyper_file = hyper_file
        self._hyper_file_downloaded = None
        self._hyper_file_extracted_path = None
        self._group = None
        self._current_group_users = {}
        self._all_users = {}
        self._new_group_users = []
        self.users_to_be_activated = []
        self.users_to_be_deactivated = []
        self.users_to_be_updated = {}
        logger.info("Initialized tableau_group_users instance")

    # the first action: to extract the group user information
    def current_server_group_users(self):
        with self.server.auth.sign_in(TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, site_id=SITE_ID)):
            logger.info("Signed in to Tableau Server")
            # populate and store the specified user group
            all_groups, pagination_item = self.server.groups.get()
            for group in all_groups:
                if group.name == GROUP_NAME:
                    self._group = group
                    self._group.minimum_site_role = UNLICENSED_ROLE

            # populate and store the current group user information
            pagination_item = self.server.groups.populate_users(self._group)
            for user in self._group.users:
                if user.site_role == UNLICENSED_ROLE:
                    continue
                self._current_group_users[user.name] = user.id

            # populate and store the site group user information
            all_users, pagination_item = self.server.users.get(TSC.RequestOptions(pagesize=PAGESIZE))
            for user in all_users:
                self._all_users[user.name] = user.id
                # print("users in the site: {}".format(user.name))
                
            # download the hyper file from the Tableau server
            if self._hyper_file:
                # all_datasources, pagination_item = self.server.datasources.get()
                all_datasources, pagination_item = self.server.datasources.get(TSC.RequestOptions(pagesize=PAGESIZE))
                print("\nThere are {} data sources on site. ".format(pagination_item.total_available))
                # print([datasource.name for datasource in all_datasources])
                file_name = self._hyper_file.split(".")[0]
                output_folder = os.getcwd()
                for datasource in all_datasources:
                    if datasource.name == file_name:
                        file_path = self.server.datasources.download(datasource.id, filepath=file_name + ".tdsx")
                        if file_path:
                            print("An updated hyper file {} has been downloaded. ".format(self._hyper_file))
                            logger.info("The updated hyper file {} has been downloaded, to {}. ".format(self._hyper_file, file_path))

                # print(os.path.join(output_folder, file_name + ".tdsx"))
                with zipfile.ZipFile(os.path.join(output_folder, file_name + ".tdsx"), "r") as zip_ref:
                    zip_ref.extractall(output_folder)

                self._hyper_file_extracted_path = os.path.join(output_folder, "Data", "Extracts", "hyper_0.hyper")
                if not os.path.isfile(os.path.join(output_folder, "Data", "Extracts", "hyper_0.hyper")):
                    logger.error("The hyper file does not exist. Please check the file path to debug.")
                    exit_program()

    # get the user list that need to be updated
    def get_user_list_to_update(self):
        if self._hyper_file_extracted_path.endswith('.hyper'):
            with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(hyper.endpoint, self._hyper_file_extracted_path, CreateMode.NONE) as connection:
                    # Read all data from the table.
                    my_data = connection.execute_list_query(""" SELECT * FROM "Extract"."Extract" """)
                    for user in my_data:
                        if not user:
                            continue
                        try:
                            self._new_group_users.append(user[2])  # the column index that includes the emp numbers
                        except IndexError:
                            logger.exception("The column index for EMP No is not valid. Program exits.")
                            exit_program()
        if not self._new_group_users:
            logger.warning("The user list is empty. Please double-check the column index.")
        else:
            logger.info("Retrieved user list to update: {}".format(self._new_group_users))

    @property
    def current_group_users(self):
        return self._current_group_users.keys()

    @property
    def new_group_users(self):
        return self._new_group_users

    def compare_users_list(self):
        self.current_server_group_users()
        self.get_user_list_to_update()
        self.users_to_be_activated = list(set(self._new_group_users) - set(self._current_group_users))
        self.users_to_be_deactivated = list(set(self._current_group_users) - set(self._new_group_users))

        for user_name in self.users_to_be_activated:
            self.users_to_be_updated[user_name] = "to_be_activated"
        for user_name in self.users_to_be_deactivated:
            self.users_to_be_updated[user_name] = "to_be_deactivated"

        with self.server.auth.sign_in(TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, site_id=SITE_ID)):
            for user_name in list(self.users_to_be_updated.keys()):
                if user_name not in self._all_users:
                    newU = TSC.UserItem(user_name, UNLICENSED_ROLE)
                    newU = self.server.users.add(newU)
                    self._all_users[user_name] = newU.id

    def update_user_roles(self):
        with self.server.auth.sign_in(TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, site_id=SITE_ID)):
            updated_account = 0
            for user_name, action in self.users_to_be_updated.items():
                user = self.server.users.get_by_id(self._all_users[user_name])

                if user.site_role.endswith("Administrator"):
                    logger.info(f"Only another administrator is allowed to change the site role for user {user_name}.")
                    continue

                if action == "to_be_activated":
                    # update the user to be view site role
                    print("activating the user {}".format(user_name))
                    logger.info("activating the user {}".format(user_name))
                    user.site_role = VIEW_ROLE
                else:
                    # change the user to be unlicensed site role
                    print("deactivating the user {}".format(user_name))
                    logger.info("deactivating the user {}".format(user_name))
                    user.site_role = UNLICENSED_ROLE

                # Remove Groups User
                try:
                    self.server.groups.remove_user(self._group, self._all_users[user_name])
                except Exception:
                    logger.debug('Error with removing the users')

                # Update User
                updated_user = self.server.users.update(user)

                # Add Groups User
                try:
                    self.server.groups.add_user(self._group, self._all_users[user_name])
                except Exception:
                    logger.debug('Error with adding the users')

                updated_account += 1

            print("{} accounts have been updated".format(updated_account))
            logger.info("{} account roles have been updated to the server".format(updated_account))


# Main program flow
def main():
    with open('tsc.log', 'a') as f:
        f.write("-" * 120 + "\n")

    try:
        # Connect to Tableau Server
        group_accounts = tableau_group_users(GROUP_NAME, SERVER_URL, TOKEN_NAME, TOKEN_SECRET, SITE_ID,
                                             hyper_file=HYPER_FILE)
        logger.info("Connected to Tableau Server")

        # compare the proposed group user and the current group and update accordingly
        group_accounts.compare_users_list()
        logger.info("Comparing proposed and current group users")
        print("Proposed group users: {}".format(group_accounts.new_group_users))
        print("Current group users: {}".format(group_accounts.current_group_users))

        group_accounts.update_user_roles()
        logger.info("Updated user roles in the group")
    except Exception as e:
        logger.exception("An error occurred: {}".format(e))
        print("An error occurred. Check the log file for details.")

    logger.info("Script execution completed")


def exit_program():
    print("Exiting the program...")
    sys.exit(0)


if __name__ == '__main__':
    main()
