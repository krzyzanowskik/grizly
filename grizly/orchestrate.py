import pandas as pd
import dask
import logging
import os
import json
import datetime
import graphviz

from time import time, sleep
from croniter import croniter
from datetime import timedelta
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import Credentials, Account, Message, HTMLBody, Configuration, DELEGATE, FaultTolerance
from grizly.etl import df_to_s3, s3_to_rds
from grizly.qframe import QFrame
from grizly.utils import read_config
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from json.decoder import JSONDecodeError
from dask.optimization import key_split
from dask.dot import _get_display_cls
from dask.core import get_dependencies


class Schedule:
    """
    Cron schedule.
    Args:
        - cron (str): cron string
        - start_date (datetime, optional): an optional schedule start date
        - end_date (datetime, optional): an optional schedule end date
    Raises:
        - ValueError: if the cron string is invalid
    """


    def __init__(self, cron, start_date=None, end_date=None):
        if not croniter.is_valid(cron):
            raise ValueError("Invalid cron string: {}".format(cron))
        self.cron = cron
        self.start_date = start_date
        self.end_date = end_date


    def emit_dates(self, start_date=None):
        """
        Generator that emits workflow run dates
        Args:
            - start_date (datetime, optional): an optional schedule start date
        Returns:
            - Iterable[datetime]: the next scheduled dates
        """
        if self.start_date:
            start_date = pendulum.instance(start_date)
        else:
            start_date = pendulum.now()

        cron = croniter(self.cron, start_date)

        while True:
            next_date = pendulum.instance(cron.get_next(datetime.datetime))
            if self.end_date and next_date > self.end_date:
                break
            yield next_date


    def next(self, n):

        dates = []
        for date in self.emit_dates(start_date=self.start_date):
            dates.append(date)
            if len(dates) == n:
                break

        return dates


class Listener:
    """A class that listens for changes in a table and server as a trigger for upstream-dependent workflows.
    
    Check and stores table's last refresh date.
    """


    def __init__(self, workflow, schema, table, field, db="denodo", trigger="default"):

        self.workflow = workflow
        self.name = workflow.name
        self.db = db
        self.schema = schema
        self.table = table
        self.field = field
        self.logger = logging.getLogger(__name__)
        self.last_data_refresh = self.get_last_refresh() # has to be standardized to one data type (currently can be int, e.g. fiscal day, or date)
        self.engine = self.get_engine()
        self.trigger = trigger

    def get_engine(self):

        config = read_config()
        engine_str = config[self.db]
        engine = create_engine(engine_str, encoding='utf8', poolclass=NullPool)

        return engine

    def get_last_refresh(self):

        with open(r"C:\Users\te393828\acoe_projects\infrastructure\listener_store.json") as f:

            try:
                listener_store = json.load(f)
                last_data_refresh = listener_store[self.name]["last_data_refresh"] # int or serialized date
            except (KeyError, JSONDecodeError) as e:
                return None

            try:
                # convert the serialized datetime to a date object
                last_data_refresh = datetime.datetime.date(datetime.datetime.strptime(last_data_refresh, r"%Y-%m-%d"))
            except:
                pass

        return last_data_refresh
            

    def update_json(self):

        with open(r"C:\Users\te393828\acoe_projects\infrastructure\listener_store.json") as json_file:
            try:
                listener_store = json.load(json_file)
            except JSONDecodeError:
                listener_store = {}

        if isinstance(self.last_data_refresh, datetime.date):
            listener_store[self.name] = {"last_data_refresh": str(self.last_data_refresh)}
        else:
            listener_store[self.name] = {"last_data_refresh": self.last_data_refresh}

        with open(r"C:\Users\te393828\acoe_projects\infrastructure\listener_store.json", "w") as f_write:
            json.dump(listener_store, f_write)

  
    
    def get_table_last_refresh(self):

        sql = f"SELECT {self.field} FROM {self.schema}.{self.table} ORDER BY {self.field} DESC LIMIT 1;"


        con = get_con(engine=self.engine)
        cursor = con.cursor()
        cursor.execute(sql)
        last_data_refresh = cursor.fetchone()[0]

        cursor.close()
        con.close()

        if isinstance(last_data_refresh, datetime.datetime):
            last_data_refresh = datetime.datetime.date(last_data_refresh)

        return last_data_refresh

    
    def should_trigger(self, table_last_refresh):

        if self.trigger == "default":
            # will trigger on day change
            if (not self.last_data_refresh) or (table_last_refresh != self.last_data_refresh):
                return True

        elif isinstance(self.trigger, Schedule):
            next_check_on = datetime.date(self.trigger.next_run)
            if next_check_on == table_last_refresh:
                return True
        if self.trigger == "fiscal day":
            # date_fiscal_day = to_fiscal(table_last_refresh, "day")
            # date_fiscal_week = to_fiscal(table_last_refresh, "week")
            # date_json_fiscal_day = to_fiscal(self.last_refresh_date, "day")
            # date_json_fiscal_week = to_fiscal(self.last_refresh_date, "week")
            # if (date_fiscal_day == date_json_fiscal_day) and (date_fiscal_day != date_json_fiscal_week): # means a week has passed
            #     return True
            pass
        elif self.trigger == "fiscal week":
            # date_fiscal_week = to_fiscal(table_last_refresh, "week")
            pass
        elif self.trigger == "fiscal year":
            # date_fiscal_year = to_fiscal(table_last_refresh, "week")
            pass


    def detect_change(self):

        self.logger.info(f"Listening for changes in {self.table}...")

        try:
            last_data_refresh = self.get_table_last_refresh()
        except:
            self.logger.exception(f"Connection or query error when connecting to {self.db}")
            last_data_refresh = None


        if self.should_trigger(last_data_refresh):
            self.last_data_refresh = last_data_refresh
            self.update_json()
            return True

        return False


class Workflow:
    """
    A class for running Dask tasks.
    """


    def __init__(self, name, owner_email, backup_email, tasks):
        self.name = name
        self.owner_email = owner_email
        self.backup_email = backup_email
        self.tasks = tasks
        self.graph = dask.delayed()(self.tasks)
        self.run_time = 0
        self.status = "idle"
        self.is_scheduled = False
        self.logger = logging.getLogger(__name__)

        self.logger.info(f"\nWorkflow {self.name} initiated successfully\n")


    def __str__(self):
        return self.tasks


    def visualize(self):
        return self.graph.visualize()


    def add_schedule(self, schedule):
        self.schedule = schedule
        self.next_run = self.schedule.next(1)[0]
        self.is_scheduled = True


    def add_listener(self, listener):
        self.listener = listener


    def retry_task(exceptions, tries=4, delay=3, backoff=2):
        """
        Retry calling the decorated function using an exponential backoff.

        Args:
            exceptions: The exception to check. may be a tuple of
                exceptions to check.
            tries: Number of times to try (not retry) before giving up.
            delay: Initial delay between retries in seconds.
            backoff: Backoff multiplier (e.g. value of 2 will double the delay
                each retry).
            logger: Logger to use. If None, print.
        """
        def deco_retry(f):

            @wraps(f)
            def f_retry(*args, **kwargs):

                mtries, mdelay = tries, delay

                while mtries > 1:

                    try:
                        return f(*args, **kwargs)

                    except exceptions as e:
                        msg = f'{e}, \nRetrying in {mdelay} seconds...'
                        self.logger.warning(msg)
                        sleep(mdelay)
                        mtries -= 1
                        mdelay *= backoff

                return f(*args, **kwargs)

            return f_retry  # true decorator

        return deco_retry


    @retry_task(Exception, tries=3, delay=300)
    def write_status_to_rds(self, name, owner_email, backup_email, status, run_time):

        schema = "administration"
        table = "status"

        last_run_date = pd.datetime.utcnow()

        status_data = {
            "workflow_name": [name],
            "owner_email": [owner_email],
            "backup_email": [backup_email],
            "run_date": [last_run_date],
            "workflow_status": [status],
            "run_time": [run_time]
        }

        status_data = pd.DataFrame(status_data)

        try:
            df_to_s3(status_data, table, schema, if_exists="append")
        except Exception as e:
            self.logger.exception(f"{self.name} status could not be uploaded to S3")
            return None

        s3_to_rds(file_name=table+".csv", schema=schema, if_exists="append")

        self.logger.info(f"{self.name} status successfully uploaded to Redshift")

        return None


    def send_email(self, body, to, cc, status):

        BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter # change this in the future to avoid warnings
        credentials = Credentials('michal.zawadzki@te.com', 'Dell4rte92q1!')
        config = Configuration(server='smtp.office365.com', credentials=credentials, retry_policy=FaultTolerance(max_wait=60*5))
        account = Account(primary_smtp_address='michal.zawadzki@te.com', credentials=credentials, config=config, autodiscover=False, access_type=DELEGATE)

        subject = f"Workflow {status}"

        m = Message(
            account=account,
            subject=subject,
            body=body,
            to_recipients=to,
            cc_recipients=cc,
        )

        try:
            m.send()
        except Exception as e:
            self.logger.exception(f"{self.name} email notification not sent.")

        return None


    def run(self):

        start = time()

        try:
            graph = dask.delayed()(self.tasks)
            graph.compute(scheduler='threads') # may need to use client.compute() for speedup and larger-than-memory datasets
            self.status = "success"
        except Exception as e:
            self.logger.exception(f"{self.name} failed")
            self.status = "fail"

        end = time()
        self.run_time = int(end-start)

        self.write_status_to_rds(self.name, self.owner_email, self.backup_email, self.status, self.run_time)

        # only send email notification on failure
        if self.status == "fail":

            run_time_str = str(timedelta(seconds=self.run_time))

            if self.is_scheduled:
                email_body = f"Scheduled workflow {self.name} has finished in {run_time_str} with the status {self.status}"
            else:
                email_body = f"""Dependent workflow {self.name} has finished in {run_time_str} with the status {self.status}.
                \nTrigger: {self.listener.table} {self.listener.field}'s latest value has changed to {self.listener.last_data_refresh}"""

            cc = self.backup_email
            to = self.owner_email
            if not isinstance(self.backup_email, list):
                cc = [self.backup_email]
            if not isinstance(self.owner_email, list):
                to = [self.owner_email]
            
            self.send_email(body=email_body, to=to, cc=cc, status=self.status)

        return self.status


class Runner:
    """Workflow runner"""


    def __init__(self, workflows, logger=None):
        self.workflows = workflows
        self.logger = logging.getLogger(__name__)


    def should_run(self, workflow):
        """Determines whether a workflow should run based on its next scheduled run.

        Parameters
        ----------
        workflow : Workflow
            A workflow instance. This function assumes the instance is generated on scheduler tick.
            Currently, this is accomplished by calling wf.generate_workflow() on each workflow every time Runner.get_pending_workflows() is called,
            which means wf.next_run recalculated every time should_run() is called.

        Returns
        -------
        boolean
            Whether the workflow's scheduled next run coincides with current time
        """

        if workflow.is_scheduled:
            now = pendulum.now()
            next_run = workflow.next_run
            self.logger.info(f"Determining whether scheduled workflow {workflow.name} shuld run... (next scheduled run: {next_run})")
            if (next_run.day == now.day) and (next_run.hour == now.hour): #and (next_run.minute == now.minute): # minutes for precise scheduling
                workflow.next_run = workflow.schedule.next(1)[0]
                return True
        else:
            listener = workflow.listener
            if listener.detect_change():
                return True

        return False


    def get_pending_workflows(self, pending=None):

        pending = []
        for workflow in self.workflows:
            if self.should_run(workflow):
                pending.append(workflow)

        return pending


    def run(self, workflows):

        for workflow in workflows:
            self.logger.info(f"Running {workflow.name}...")
            status = workflow.run()

        return {workflow.name: workflow.status for workflow in workflows}


class SimpleGraph:
    """Produces a simplified Dask graph"""


    def __init__(self, format="png", filename=None):
        self.format = format
        self.filename = filename

    @staticmethod
    def _node_key(s):
        if isinstance(s, tuple):
            return s[0]
        return str(s)

    def visualize(self,
                     x,
                     filename='simple_computation_graph',
                     format=None):

        if hasattr(x, 'dask'):
            dsk = x.__dask_optimize__(x.dask, x.__dask_keys__())
        else:
            dsk = x

        deps = {k: get_dependencies(dsk, k) for k in dsk}

        g = graphviz.Digraph(graph_attr={'rankdir': 'LR'})

        nodes = set()
        edges = set()
        for k in dsk:
            key = self._node_key(k)
            if key not in nodes:
                g.node(key, label=key_split(k), shape='rectangle')
                nodes.add(key)
            for dep in deps[k]:
                dep_key = self._node_key(dep)
                if dep_key not in nodes:
                    g.node(dep_key, label=key_split(dep), shape='rectangle')
                    nodes.add(dep_key)
                # Avoid circular references
                if dep_key != key and (dep_key, key) not in edges:
                    g.edge(dep_key, key)
                    edges.add((dep_key, key))

        data = g.pipe(format=self.format)
        display_cls = _get_display_cls(self.format)

        if self.filename is None:
            return display_cls(data=data)

        full_filename = '.'.join([filename, self.format])
        with open(full_filename, 'wb') as f:
            f.write(data)

        return display_cls(filename=full_filename)


def retry(exceptions, tries=4, delay=3, backoff=2):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.


    This is almost a copy of Workflow.retry, but it's using its own logger.
    """

    logger = logging.getLogger(__name__)

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):

            mtries, mdelay = tries, delay

            while mtries > 1:

                try:
                    return f(*args, **kwargs)

                except exceptions as e:
                    msg = f'{e}, \nRetrying in {mdelay} seconds...'
                    logger.warning(msg)
                    sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff

            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry(Exception, tries=5, delay=5)
def get_con(engine):
    con = engine.connect().connection
    return con
