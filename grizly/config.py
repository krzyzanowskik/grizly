import json
from .utils import get_path

class Config():
    """Class which stores grizly configuration"""
    data = {}

    def from_dict(self, data:dict):
        """Overwrites Config.data using dictionary data

        Parameters
        ----------
        data : dict
            Dictionary with config key.

        Examples
        --------
        >>> standard = {
        ...        "config": {
        ...            "standard": {
        ...            "email": {
        ...                "email_address": "my_email@example.com",
        ...                "email_password": "my_password",
        ...                "send_as": "Team"
        ...            },
        ...            "github": {
        ...                "username": "my_login",
        ...                "username_password": "my_password",
        ...                "proxies": {
        ...                     "http": "first_proxy",
        ...                     "https": "second_proxy"
        ...                },
        ...                "pages": 100
        ...            },
        ...            "sfdc": {
        ...                "stage": {
        ...                "username": "my_login",
        ...                "instance_url": "https://na1.salesforce.com",
        ...                "password": "my_password",
        ...                "organizationId": "OrgId"
        ...                },
        ...                "prod": {
        ...                "username": "my_login",
        ...                "password": "my_password",
        ...                "organizationId": "OrgId"
        ...                }
        ...            }
        ...            }
        ...        }
        ...        }
        >>> conf = Config().from_dict(standard)
        Config data has been saved.

        Returns
        -------
        Config
        """
        if 'config' in data:
            if not isinstance(data['config'], dict): raise TypeError("config must be a dictionary")
            if data['config'] == {}: raise ValueError("config is empty")

            for key in data['config'].keys():
                _validate_config(data['config'][key], services = list(data['config'][key]))

            Config.data = data['config']
            print("Config data has been saved.")
            return Config()
        else:
            raise KeyError("'config' key not found")


    def from_json(self, json_path:str):
        """Overwrites Config.data using dictionary data

        Parameters
        ----------
        json_path : str
            Path to json file

        Examples
        --------
        >>> json_path = get_path('grizly', 'notebooks', 'config.json')
        >>> conf = Config().from_json(json_path)
        Config data has been saved.

        Returns
        -------
        Config
        """
        with open(json_path, 'r') as json_file:
            data = json.load(json_file)
        if 'config' in data:
            if not isinstance(data['config'], dict): raise TypeError("config must be a dictionary")
            if data['config'] == {}: raise ValueError("config is empty")

            for key in data['config'].keys():
                _validate_config(data['config'][key], services = list(data['config'][key]))

            Config.data = data['config']
            print("Config data has been saved.")
            return Config()
        else:
            raise KeyError("'config' key not found")


    def add_keys(self, data:dict, if_exists:str='skip'):
        """Adds new keys to Config.data

        Parameters
        ----------
        data : dict
            Dictionary with keys to be added to Config.data
        if_exists : {'skip', 'replace'}, default 'skip'
            How to behave if the key already exists.

            * skip: Skips existing key
            * replace: Replaces existing key

        Examples
        --------
        >>> personal_john = {
        ...        "personal_john": {
        ...        "email": {
        ...            "email_address": "john_snow@example.com",
        ...            "email_password": "wolf123",
        ...            "send_as": "John Snow"
        ...        }
        ...        }
        ...    }
        >>> conf = Config().add_keys(personal_john)
        Key 'personal_john' has been added.

        Returns
        -------
        Config
        """
        if if_exists not in ("skip", "replace"):
            raise ValueError("'{}' is not valid for if_exists. Valid values: 'skip', 'replace'".format(if_exists))
        for key in data.keys():
            if key in list(Config.data.keys()):
                if if_exists == 'skip':
                    print(f"Key '{key}' already exists and has been skipped. If you want to overwrite it please use if_exists='replace'")
                elif if_exists == 'replace':
                    _validate_config(data[key], services = list(data[key]))
                    Config.data.update({key: data[key]})
                    print(f"Key '{key}' has been overwritten.")
            else:
                _validate_config(data[key], services = list(data[key]))
                Config.data[key] = data[key]
                print(f"Key '{key}' has been added.")
        return Config()


def _validate_config(config:dict, services:list=None, env:str=None):
    """Validates config dictionary.

    Parameters
    ----------
    config : dict
        Config to validate
    service : str or list
        Services to validate, options: 'email', 'github', 'sfdc', if None then ['email', 'github', 'sfdc']
    env : str
        ONLY FOR service='sfdc', options:

        * 'prod'
        * 'stage'
        * None: In this case only checks if config['sfdc'] contain 'stage' or 'prod' and validate what he founds

    Returns
    -------
    dict
        Validated config
    """

    if not isinstance(config, dict): raise TypeError("config must be a dictionary")
    if config == {}: raise ValueError("config is empty")

    invalid_keys = set(config.keys()) - {'email', 'github', 'sfdc'}
    if invalid_keys != set() :
        raise KeyError(f"Invalid keys {invalid_keys} in config. Valid keys: 'email', 'github', 'sfdc'")

    if services == None: services = ['email', 'github', 'sfdc']
    if isinstance(services, str): services = [services]
    if not isinstance(services, list) : raise TypeError("services must be a list or string")

    invalid_services = set(services) - {'email', 'github', 'sfdc'}
    if invalid_services != set():
        raise ValueError(f"Invalid values in services {invalid_services}. Valid values: 'email', 'github', 'sfdc'")

    if env not in ("prod", "stage", None):
        raise ValueError("'{}' is not valid for env. Valid values: 'prod', 'stage'".format(env))

    for service in services:
        if service not in config.keys(): raise KeyError(f"'{service}' not found in config")

        if service == 'email':
            if not isinstance(config['email'], dict): raise TypeError ("config['email'] must be a dictionary")
            if config['email'] == {}: raise ValueError("config['email'] is empty")

            invalid_keys = set(config['email'].keys()) - {'email_address', 'email_password', 'send_as'}
            if invalid_keys != set():
                raise KeyError(f"Invalid keys {invalid_keys} in config['email']. Valid keys: 'email_address', 'email_password', 'send_as'")

            not_found_keys = {'email_address', 'email_password', 'send_as'} - set(config['email'].keys())
            if not_found_keys != set():
                raise KeyError(f"Keys {not_found_keys} not found in config['email']")

        elif service == 'github':
            if not isinstance(config['github'], dict): raise TypeError ("config['github'] must be a dictionary")
            if config['github'] == {}: raise ValueError("config['github'] is empty")

            invalid_keys = set(config['github'].keys()) - {'username', 'username_password', 'pages', 'proxies'}
            if invalid_keys != set():
                raise KeyError(f"Invalid keys {invalid_keys} in config['github']. Valid keys: 'username', 'username_password'")

            not_found_keys = {'username', 'username_password', 'pages', 'proxies'} - set(config['github'].keys())
            if not_found_keys != set():
                raise KeyError(f"Keys {not_found_keys} not found in config['github']")

        elif service == 'sfdc':
            if not isinstance(config['sfdc'], dict): raise TypeError ("config['sfdc'] must be a dictionary")
            if config['sfdc'] == {}: raise ValueError("config['sfdc'] is empty")

            invalid_keys = set(config['sfdc'].keys()) - {'stage', 'prod'}
            if invalid_keys != set():
                raise KeyError(f"Invalid keys {invalid_keys} in config['sfdc']. Valid keys: 'prod', 'stage'")

            if env == "stage":
                if "stage" in config['sfdc'].keys():
                    if not isinstance(config['sfdc']['stage'], dict): raise TypeError ("config['sfdc']['stage'] must be a dictionary")
                    if config['sfdc']['stage'] == {}: raise ValueError("config['sfdc']['stage'] is empty")

                    invalid_keys = set(config['sfdc']['stage'].keys()) - {'username', 'password', 'instance_url', 'organizationId'}
                    if invalid_keys != set():
                        raise KeyError(f"Invalid keys {invalid_keys} in config['sfdc']['stage']. Valid keys: 'username', 'password', 'instance_url', 'organizationId'")

                    not_found_keys = {'username', 'password', 'instance_url', 'organizationId'} - set(config['sfdc']['stage'].keys())
                    if not_found_keys != set():
                        raise KeyError(f"Keys {not_found_keys} not found in config['sfdc']['stage']")
                else:
                    raise KeyError("Key 'stage' not found in config['sfdc']")

            elif env == "prod":
                if "prod" in config['sfdc'].keys():
                    if not isinstance(config['sfdc']['prod'], dict): raise TypeError ("config['sfdc']['prod'] must be a dictionary")
                    if config['sfdc']['prod'] == {}: raise ValueError("config['sfdc']['prod'] is empty")

                    invalid_keys = set(config['sfdc']['prod'].keys()) - {'username', 'password', 'organizationId'}
                    if invalid_keys != set():
                        raise KeyError(f"Invalid keys {invalid_keys} in config['sfdc']['prod']. Valid keys: 'username', 'password', 'organizationId'")

                    not_found_keys = {'username', 'password', 'organizationId'} - set(config['sfdc']['prod'].keys())
                    if not_found_keys != set():
                        raise KeyError(f"Keys {not_found_keys} not found in config['sfdc']['prod']")
                else:
                    raise KeyError("Key 'prod' not found in config['sfdc']")
            else:
                pass
    return config



# if __name__ == "__main__":
#     import doctest
#     doctest.testmod()

