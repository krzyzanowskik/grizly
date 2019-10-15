import pendulum
import pandas as pd
import dask
import logging
import os
import json

from time import time, sleep
from croniter import croniter
from datetime import datetime, timedelta
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import Credentials, Account, Message, HTMLBody, Configuration, DELEGATE, FaultTolerance
from grizly import df_to_s3, s3_to_rds, QFrame, read_config
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from json.decoder import JSONDecodeError


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
            next_date = pendulum.instance(cron.get_next(datetime))
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
    """A class that listens for changes in a table
    """


    def __init__(self, workflow, schema, table, field, db="denodo"):

        self.workflow = workflow
        self.name = workflow.name
        self.db = db
        self.schema = schema
        self.table = table
        self.field = field
        self.last_data_refresh = self.get_last_refresh()
        self.engine = self.get_engine()
        self.logger = logging.getLogger(__name__)
        #self.trigger = trigger

    def get_engine(self):

        config = read_config()
        engine_str = config[self.db]
        engine = create_engine(engine_str, encoding='utf8', poolclass=NullPool)

        return engine

    def get_last_refresh(self):

        with open(r"C:\Users\te393828\acoe_projects\infrastructure\listener_store.json") as f:
            try:
                listener_store = json.load(f)
                last_data_refresh = listener_store[self.name]["last_data_refresh"]
                return last_data_refresh
            except (KeyError, JSONDecodeError):
                return None

    def update_json(self):

        with open(r"C:\Users\te393828\acoe_projects\infrastructure\listener_store.json") as json_file:
            try:
                listener_store = json.load(json_file)
            except JSONDecodeError:
                listener_store = {}
            listener_store[self.name] = {"last_data_refresh": self.last_data_refresh}
        with open(r"C:\Users\te393828\acoe_projects\infrastructure\listener_store.json", "w") as f_write:
            json.dump(listener_store, f_write)

    def detect_change(self):

        self.logger.info(f"Listening for changes in {self.table}...")

        sql = f"SELECT {self.field} FROM {self.schema}.{self.table} ORDER BY {self.field} DESC LIMIT 1;"

        try:
            con = self.engine.connect().connection
            cursor = con.cursor()
            cursor.execute(sql)
            last_data_refresh = cursor.fetchone()[0]
        except:
            self.logger.exception(f"Connection or query error when connecting to {self.db}")

        if (not self.last_data_refresh) or (last_data_refresh != self.last_data_refresh):
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

        schema = "z_sandbox_mz"
        table = "status"

        last_run_date = pd.datetime.utcnow()

        status_data = {
            "job_name": [name],
            "owner_email": [owner_email],
            "backup_email": [backup_email],
            "run_date": [last_run_date],
            "flow_status": [status],
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

        run_time_str = str(timedelta(seconds=self.run_time))
        email_body = f"Workflow {self.name} has finished in {run_time_str} with the status {self.status}"


        self.write_status_to_rds(self.name, self.owner_email, self.backup_email, self.status, self.run_time)
        self.send_email(body=email_body, to=[self.owner_email], cc=[self.backup_email], status=self.status)

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
            self.logger.info(f"Determining whether {workflow.name} shuld run... (next scheduled run: {next_run})")
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
