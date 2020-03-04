import pandas
import requests
from ..config import Config, _validate_config
from .extract import Extract
from .s3 import S3


class GitHub(Extract):

    def __init__(self, username:str=None, username_password:str=None, pages:int=100
                    , proxies:dict=None, config_key="standard"):
        """Pulls GitHub data into a pandas data frame
        
        Parameters
        ----------
        username : str
            [description]
        username_password : str
            [description]
        pages : int, optional
            [description], by default 100
        """
        super().__init__()
        
        if username_password is None:
            _validate_config(config=Config.data[config_key],
                            services='github')
            config = Config.data[config_key]['github']
            self.config = config
            self.username =  config['username']
            self.username_password = config['username_password']
            self.pages = config['pages']
            self.proxies = config['proxies']
        else:
            self.username = username
            self.username_password = username_password
            self.pages = pages
            self.proxies = proxies
            self.config = None
        self.df = None

    def from_issues(self, org_name:str):
        """Gets issues into a data frame
        
        Parameters
        ----------
        org_name : str
            [name of the github org]
        
        Returns
        -------
        self, do self.data for the dataframe
        """
        proxies = {
            "http": "http://restrictedproxy.tycoelectronics.com:80",
            "https": "https://restrictedproxy.tycoelectronics.com:80",
            }
        records = []
        if self.username is None:
            self.username = _validate_config(config=Config.data[self.config_key], 
                                        services='github')['github']['username']
        if self.username_password is None:
            self.username_password = _validate_config(config=Config.data[self.config_key], 
                                                services='github')['github']['username_password']

        for page in range(self.pages):
            page += 1
            issues = f'https://api.github.com/orgs/{org_name}/issues?page={page}&filter=all'
            data = requests.get(issues, auth=(self.username,self.username_password), proxies = self.proxies)
            if len(data.json()) == 0:
                break
            if page == 1:
                records.append(["url","repository_name", "user_login", "assignees_login"
                , "milestone_title", "title", "created_at", "updated_at", "state", "labels"])
            for i in range(len(data.json())):
                record = []
                record.append(data.json()[i]["url"])
                record.append(data.json()[i]["repository"]["name"])
                record.append(data.json()[i]["user"]["login"])
                record.append(', '.join([assignee["login"] for assignee in data.json()[i]["assignees"]]))
                try:
                    record.append(data.json()[i]["milestone"]["title"])
                except:
                    record.append("no_milestone")
                record.append(data.json()[i]["title"])
                record.append(data.json()[i]["created_at"])
                record.append(data.json()[i]["updated_at"])
                record.append(data.json()[i]["state"])
                record.append(', '.join([label["name"] for label in data.json()[i]["labels"]]))
                records.append(record)

        self.df = pandas.DataFrame(records[1:])
        self.df.columns = records[0]

        return self
