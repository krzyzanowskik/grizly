import csv
import logging
import os

import numpy as np
import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.login import SalesforceAuthenticationFailed


class SFDC:
    """A class for extracting with Salesforce data.

    -------
    Usage:

    query = "SELECT Id FROM User"
    response = SFDC(username=usr, password=pw, organization_id=org_id).query(query)
    response.to_csv(file_path)
    # df = response.to_df()
    """
    
    def __init__(self, username, password, organization_id=None, instance_url=None, env="prod", logger=None):
        self.username = username
        self.password = password
        self.organization_id = organization_id
        self.instance_url = instance_url
        self.env = env
        self.logger = logger if logger else logging.getLogger(__name__)
        self.http_proxy = r"http://restrictedproxy.tycoelectronics.com:80" # os.getenv("HTTP_PROXY") or None
        self.https_proxy = r"http://restrictedproxy.tycoelectronics.com:80" # os.getenv("HTTPS_PROXY") or None
        self.proxies = {
            "http": self.http_proxy,
            "https": self.https_proxy,
        }

    def _connect(self):

        if self.env == "prod":
            try:
                sf_conn = Salesforce(username=self.username, password=self.password, organizationId=self.organization_id, proxies=self.proxies)
            except SalesforceAuthenticationFailed:
                self.logger.exception("Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?")
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
                self.logger.exception("Could not log in to SFDC. Are you sure your password hasn't expired and your proxy is set up correctly?")
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
