import csv
import logging
import os

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed
from .config import Config
from .utils import get_path
from logging import Logger

class SFDC:
    """A class for extracting with Salesforce data.

    Examples:
    -------
    >>> from grizly.sfdc import SFDC
    >>> from grizly.utils import get_path
    >>> from grizly.config import Config
    >>> 
    >>> organization_id = "00DE0000000Hkve"
    >>> 
    >>> config_path = get_path('.grizly', 'config.json') # contains SFDC username and password
    >>> Config().from_json(config_path)
    >>> 
    >>> query = "SELECT Id FROM User LIMIT 3"
    >>> 
    >>> sf = SFDC(organization_id=organization_id)
    >>> response = sf.query(query).to_df()
    >>> 
    >>> len(response)
    3
    
    Returns
    -------
    CSV or DataFrame
    """

    def __init__(self, username: str=None, password: str=None, organization_id: str=None,
                instance_url: str=None, env="prod", logger: Logger=None, proxies: dict=None,
                config_key: str="standard"):

        self.global_config = self.get_config(config_key)
        self.config = self.get_config(config_key, service="sfdc")
        # first lookup in parameters, then config, then env variables
        self.proxies = proxies or self.get_config_proxies() or {
            "http": os.getenv("HTTP_PROXY"),
            "https": os.getenv("HTTPS_PROXY"),
        }
        self.username = username or self.config[env]["username"]
        self.password = password or self.config[env]["password"]
        self.organization_id = organization_id or self.config[env]["organizationId"]
        self.instance_url = instance_url
        self.env = env
        self.logger = logger if logger else logging.getLogger(__name__)

    def _validate_proxies(proxies):
        if not (proxies["http"] or proxies["https"]):
            self.proxies = None

    def get_config_proxies(self):
        try:
            proxies = self.global_config["proxies"]
        except KeyError:
            return None
        if not (proxies["http"] and proxies["https"]):
            return None
        return proxies

    def get_config(self, config_key, service=None):
        config_path = get_path('.grizly', 'config.json')
        if service:
            config = Config().from_json(config_path).data[config_key][service]
        else:
            config = Config().from_json(config_path).data[config_key]
        return config

    def _connect(self):

        if self.env == "prod":
            try:
                sf_conn = Salesforce(username=self.username, password=self.password,
                                    organizationId=self.organization_id, proxies=self.proxies)
            except SalesforceAuthenticationFailed:
                self.logger.exception("Could not log in to SFDC. \
                Are you sure your password hasn't expired and your proxy is set up correctly?")
                raise

        elif self.env == "stage":
            try:
                sf_conn = Salesforce(instance_url=self.instance_url,
                                username=self.username,
                                password=self.password,
                                organizationId=self.organization_id,
                                sandbox=True,
                                security_token='',
                                proxies=self.proxies)
            except SalesforceAuthenticationFailed:
                self.logger.exception("Could not log in to SFDC. \
                Are you sure your password hasn't expired and your proxy is set up correctly?")
                raise

        else:
            raise ValueError("Only 'prod' and 'stage' environments are supported")

        return sf_conn


    def query(self, query):
        """Query a SFDC table. Only simple SELECTs are supported.

        Parameters
        ----------
        columns : list
            Columns to be passed to SELECT
        table : str
            The SFDC table to query
        env : str
            The environment on which to run query
        where : str, optional
            Where clause, by default None
        """

        sf = self._connect()

        try:
            raw_response = sf.query_all(query)
            response = SFDCResponse(raw_response, logger=self.logger)
        except:
            self.logger.exception("Error when connecting to SFDC")
            raise

        query_words = query.lower().split()
        table = query_words[query_words.index("from") + 1]
        self.logger.debug(f"SFDC table {table} was successfully queried")

        return response


class SFDCResponse:
    def __init__(self, data, logger=None):
        self.data = data
        self.columns = [item for item in data["records"][0] if item != "attributes"]
        self.logger = logger if logger else logging.getLogger(__name__)

    def __str__(self):
        return self.data

    def to_df(self):

        l = []
        for item in self.data['records']:
            row = []
            for column in self.columns:
                row.append(item[column])
            l.append(row)

        df = (pd
                .DataFrame(l, columns=self.columns)
                .replace(to_replace=["None"], value=np.nan)
                )

        return df

    def to_csv(self, file_path):

        if not file_path:
            raise ValueError("File path is required")

        rows = []
        rows.append(self.columns)
        for item in self.data['records']:
            row = []
            for column in self.columns:
                row.append(item[column])
            rows.append(row)

        with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
            self.logger.debug(f"Writing to {os.path.basename(file_path)}...")
            writer = csv.writer(csv_file, delimiter='\t')
            writer.writerows(rows)
            self.logger.debug(f"Successfuly wrote to {os.path.basename(file_path)}")
