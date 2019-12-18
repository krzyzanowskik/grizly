import pandas as pd
import dask
import logging
import os
import json
import datetime
import graphviz
import traceback
import sys

from time import time, sleep
from croniter import croniter
from datetime import timedelta
from .etl import df_to_s3, s3_to_rds
from .tools.qframe import QFrame
from .utils import read_config, get_path
from .email import Email
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from json.decoder import JSONDecodeError
from dask.optimization import key_split
from dask.dot import _get_display_cls
from dask.core import get_dependencies


def _validate_date(maybe_date):
    if not isinstance(maybe_date, (datetime.date, int)):
        return False
    return True


def cast_to_date(maybe_date):

    if isinstance(maybe_date, str):
        date = datetime.datetime.strptime(maybe_date, "%Y-%m-%d")
    elif isinstance(maybe_date, datetime.datetime):
        date = datetime.datetime.date(maybe_date)
    else:
        date = maybe_date

    if not _validate_date(date):
        raise TypeError(f"The specified trigger field is not of type (int, datetime.datetime.date)")

    return date


class Schedule:
    """
    Cron schedule.
    Args:
        - cron (str): cron string
        - start_date (datetime, optional): an optional schedule start datetime (assumed to be UTC)
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
        if not self.start_date:
            start_date = datetime.datetime.now(datetime.timezone.utc)

        cron = croniter(self.cron, start_date)

        while True:
            next_date = cron.get_next(datetime.datetime)
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


class Trigger:
    
    def __init__(self, func, params=None):
        self.check = func
        self.kwargs = params or {}
        self.should_run = self.check(**self.kwargs)


class Listener:
    """A class that listens for changes in a table and server as a trigger for upstream-dependent workflows.

    Check and stores table's last refresh date.
    """


    def __init__(self, workflow, schema, table, field=None, query=None, db="denodo", trigger=None, trigger_type="default", delay=300):

        self.workflow = workflow
        self.name = workflow.name
        self.db = db
        self.schema = schema
        self.table = table
        self.field = field
        self.query = query
        self.logger = logging.getLogger(__name__)
        self.trigger_type = trigger_type
        self.trigger = trigger
        self.last_data_refresh = self.get_last_refresh()
        self.engine = self.get_engine()
        self.last_trigger_run = self.get_last_trigger_run()
        self.delay = delay

    def retry_task(exceptions, tries=4, delay=3, backoff=2, logger=None):
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

        if not logger:
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

    def get_engine(self):

        config = read_config()
        engine_str = config[self.db]
        engine = create_engine(engine_str, encoding='utf8', poolclass=NullPool)

        return engine

    def get_last_refresh(self):
        with open("etc/listener_store.json") as f:
            listener_store = json.load(f)
            last_data_refresh = listener_store[self.name].get("last_data_refresh") # int or serialized date
            try:
                # attempt to convert the serialized datetime to a date object
                last_data_refresh = datetime.datetime.date(datetime.datetime.strptime(last_data_refresh, r"%Y-%m-%d"))
            except:
                pass
            return last_data_refresh

    def get_last_trigger_run(self):
        with open("etc/listener_store.json") as f:
            listener_store = json.load(f)
            last_trigger_run = listener_store[self.name].get("last_trigger_run")
            try:
                # attempt to convert the serialized datetime to a date object
                last_trigger_run = datetime.datetime.date(datetime.datetime.strptime(last_trigger_run, r"%Y-%m-%d"))
            except:
                pass
            return last_trigger_run

    def update_json(self):
        with open("etc/listener_store.json") as json_file:
            try:
                listener_store = json.load(json_file)
            except JSONDecodeError:
                listener_store = {}

        if self.trigger:
            listener_store[self.name] = {"last_trigger_run": str(self.last_trigger_run)}
        else:
            if isinstance(self.last_data_refresh, datetime.date):
                listener_store[self.name] = {"last_data_refresh": str(self.last_data_refresh)}
            else:
                listener_store[self.name] = {"last_data_refresh": self.last_data_refresh}

        with open("etc/listener_store.json", "w") as f_write:
            json.dump(listener_store, f_write, indent=4)

    @retry_task(TypeError, tries=3, delay=10)
    def get_table_refresh_date(self):

        if self.query:
            sql = self.query
        else:
            sql = f"SELECT {self.field} FROM {self.schema}.{self.table} ORDER BY {self.field} DESC LIMIT 1;"

        con = get_con(engine=self.engine)
        cursor = con.cursor()
        cursor.execute(sql)

        try:
            last_data_refresh = cursor.fetchone()[0]
        except TypeError:
            self.logger.exception(f"{self.name}'s trigger field is empty")
            raise

        cursor.close()
        con.close()

        # try casting to date
        last_data_refresh = cast_to_date(last_data_refresh)

        return last_data_refresh


    def should_trigger(self, table_refresh_date=None):

        if self.trigger:
            today = datetime.datetime.today().date()
            if today == self.last_trigger_run:
                return False # workflow was already ran today
            return self.trigger.should_run()

        else:
            # first run
            if not self.last_data_refresh:
                return True
            # the table ran previously, but now the refresh date is None
            # this mean the table is being cleared in preparation for update
            if not table_refresh_date:
                return False
            # trigger on day change
            if table_refresh_date != self.last_data_refresh:
                return True

        # elif isinstance(self.trigger_type, Schedule):
        #     next_check_on = datetime.date(self.trigger_type.next_run)
        #     if next_check_on == table_refresh_date:
        #         return True
        # if self.trigger_type == "fiscal day":
        #     # date_fiscal_day = to_fiscal(table_last_refresh, "day")
        #     # date_fiscal_week = to_fiscal(table_last_refresh, "week")
        #     # date_json_fiscal_day = to_fiscal(self.last_refresh_date, "day")
        #     # date_json_fiscal_week = to_fiscal(self.last_refresh_date, "week")
        #     # if (date_fiscal_day == date_json_fiscal_day) and (date_fiscal_day != date_json_fiscal_week): # means a week has passed
        #     #     return True
        #     pass
        # elif self.trigger == "fiscal week":
        #     # date_fiscal_week = to_fiscal(table_last_refresh, "week")
        #     pass
        # elif self.trigger == "fiscal year":
        #     # date_fiscal_year = to_fiscal(table_last_refresh, "week")
        #     pass

    def detect_change(self):

        if not any([self.field, self.query, self.trigger]):
            raise ValueError("Please specify the trigger for the listener")

        if self.trigger:
            if self.should_trigger():
                today = datetime.datetime.today().date()
                self.last_trigger_run = today
                self.update_json()
                self.logger.info(f"Waiting {self.delay} seconds for the table upload to be finished before runnning workflow...")
                sleep(self.delay)
                return True

        else:
            try:
                last_data_refresh = self.get_table_refresh_date()
            except:
                self.logger.exception(f"Connection or query error when connecting to {self.db}")
                last_data_refresh = None

            if self.should_trigger(last_data_refresh):
                self.last_data_refresh = last_data_refresh
                self.update_json()
                self.logger.info(f"Waiting {self.delay} seconds for the table upload to be finished before runnning workflow...")
                sleep(self.delay)
                return True

        return False


class Workflow:
    """
    A class for running Dask tasks.
    """

    def __init__(self, name, owner_email, backup_email, tasks, trigger_on_success=None,
                execution_options: dict=None):
        self.name = name
        self.owner_email = owner_email
        self.backup_email = backup_email
        self.tasks = tasks
        self.trigger_on_success = trigger_on_success
        self.execution_options = execution_options
        self.graph = dask.delayed()(self.tasks)
        self.run_time = 0
        self.status = "idle"
        self.is_scheduled = False
        self.is_triggered = False
        self.is_manual = False
        self.stage = "prod"
        self.logger = logging.getLogger(__name__)
        self.error_value = None
        self.error_type = None
        self.scheduler = "threads"
        self.num_workers = 8

        self.logger.info(f"Workflow {self.name} initiated successfully")


    def add_task(fn, max_retries=5, retry_delay=5, depends_on=None):
        tasks = [load_qf(qf, depends_on=None)]
        fnretry = retry(fn)
        self.tasks
        return fn_decorated

    def __str__(self):
        return self.tasks

    def log_task(self, task):
        def deco():
            pass

    def visualize(self):
        return self.graph.visualize()


    def add_schedule(self, schedule):
        self.schedule = schedule
        self.next_run = self.schedule.next(1)[0]
        self.is_scheduled = True


    def add_listener(self, listener):
        self.listener = listener
        self.is_triggered = True


    def check_if_manual(self):
        if not (self.is_scheduled or self.is_triggered):
            self.is_manual = True
            return True


    def retry_task(exceptions, tries=4, delay=3, backoff=2, logger=None):
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

        if not logger:
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


    @retry_task(Exception, tries=3, delay=300)
    def write_status_to_rds(self, name, owner_email, backup_email, status, run_time, env, error_value=None, error_type=None):

        schema = "administration"
        table = "status"

        last_run_date = pd.datetime.utcnow()

        status_data = {
            "workflow_name": [name],
            "owner_email": [owner_email],
            "backup_email": [backup_email],
            "run_date": [last_run_date],
            "workflow_status": [status],
            "run_time": [run_time],
            "env": [env],
            "error_value": [error_value],
            "error_type": [error_type]
        }

        status_data = pd.DataFrame(status_data)

        try:
            df_to_s3(status_data, table, schema, if_exists="append", redshift_str='mssql+pyodbc://redshift_acoe', s3_key="bulk", bucket="acoe-s3")
        except Exception as e:
            self.logger.exception(f"{self.name} status could not be uploaded to S3")
            return None

        s3_to_rds(file_name=table+".csv", schema=schema, if_exists="append", redshift_str='mssql+pyodbc://redshift_acoe', s3_key="bulk", bucket="acoe-s3")

        self.logger.info(f"{self.name} status successfully uploaded to Redshift")

        return None


    def run(self, local=False):

        if self.check_if_manual():
            pass

        start = time()

        try:
            with open("etc/cur_wf_start_time.txt", "w+") as f:
                f.write(str(start))
        except FileNotFoundError:
            with open("cur_wf_start_time.txt", "w+") as f:
                f.write(str(start))

        try:
            graph = dask.delayed()(self.tasks)
            if self.execution_options:
                scheduler = self.execution_options.get("scheduler") or self.scheduler
                num_workers = self.execution_options.get("num_workers") or self.num_workers
            else:
                scheduler = "threads"
                num_workers = self.num_workers
            graph.compute(scheduler=scheduler, num_workers=num_workers)
            self.status = "success"
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            self.error_value = str(exc_value)[:250]
            self.error_type = str(exc_type).split("'")[1] # <class 'ZeroDivisionError'> -> ZeroDivisionError
            self.error_message = traceback.format_exc()
            self.logger.exception(f"{self.name} failed")
            self.status = "fail"

        end = time()
        self.run_time = int(end-start)

        # prepare email body; to be refactored into a function
        run_time_str = str(timedelta(seconds=self.run_time))
        if self.is_scheduled:
            email_body = f"Scheduled workflow {self.name} has finished in {run_time_str} with the status {self.status}"
        elif self.is_triggered:
            email_body = f"""Dependent workflow {self.name} has finished in {run_time_str} with the status {self.status}.
            \nTrigger: {self.listener.table} {self.listener.field}'s latest value has changed to {self.listener.last_data_refresh}"""
        else:
            email_body = f"Manual workflow {self.name} has finished in {run_time_str} with the status {self.status}"
        if self.status == "fail":
            email_body += f"\n\nError message: \n\n{self.error_message}"

        cc = self.backup_email if isinstance(self.backup_email, list) else [self.backup_email]
        to = self.owner_email if isinstance(self.owner_email, list) else [self.owner_email]
        subject = f"Workflow {self.status}"

        notification = Email(subject=subject, body=email_body, logger=self.logger)
        notification.send(to=to, cc=cc, send_as="")
        # when ran on server, the status is handled by Runner
        if local:
            self.write_status_to_rds(self.name, self.owner_email, self.backup_email, self.status, self.run_time,
                                    env="local", error_value=self.error_value, error_type=self.error_type)

        if self.trigger_on_success:
            triggered_wf = self.trigger_on_success
            self.logger.info(f"Running {triggered_wf.name}...")
            triggered_wf.run()
            self.logger.info(f"Finished running {triggered_wf.name} with the status <{triggered_wf.status}>")
            triggered_wf.write_status_to_rds(triggered_wf.name, triggered_wf.owner_email, triggered_wf.backup_email,
                                            triggered_wf.status, triggered_wf.run_time, env="prod",
                                            error_value=triggered_wf.error_value, error_type=triggered_wf.error_type)

        return self.status


class Runner:
    """Workflow runner"""

    def __init__(self, workflows, logger=None, env=None):
        self.workflows = workflows
        self.env = env if env else "prod"
        self.logger = logging.getLogger(__name__)
        self.run_params=None

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

            self.logger.info(f"Determining whether scheduled workflow {workflow.name} shuld run... (next scheduled run: {workflow.next_run})")

            now = datetime.datetime.now(datetime.timezone.utc)
            next_run = workflow.next_run
            if (next_run.day == now.day) and (next_run.hour == now.hour): #and (next_run.minute == now.minute): # minutes for precise scheduling
                workflow.next_run = workflow.schedule.next(1)[0]
                return True

        elif workflow.is_triggered:

            self.logger.info(f"Listening for changes in {workflow.listener.table}...")

            listener = workflow.listener
            if listener.detect_change():
                return True

        elif workflow.is_manual:
            # implement manual run logic here
            return True

        return False

    # def get_pending_workflows(self, pending=None):

    #     pending = []
    #     for workflow in self.workflows:
    #         if self.should_run(workflow):
    #             pending.append(workflow)

    #     return pending

    def overwrite_params(self, workflow, params):
        # params: dict fof the form {"listener": {"delay": 0}, "backup_email": "test@example.com"}
        for param in params:
    
            # modify parameters of classes stored in Workflow, e.g. Listener of Schedule
            if type(params[param]) == dict:
                _class = param
                
                # only apply listener params to triggered workflows
                if _class == "listener":
                    if not workflow.is_triggered:
                        continue
                        
                # only apply schedule params to scheduled workflows
                elif _class == "schedule":
                    if not workflow.is_scheduled:
                        continue
                
                # retrieve object and names of attributes to set
                obj = eval(f"workflow.{_class}") 
                obj_params = params[param]
                
                for obj_param in obj_params:
                    new_param_value = obj_params[obj_param]
                    setattr(obj, obj_param, new_param_value)
                    
            # modify Workflow object's parameters
            else:
                setattr(workflow, param, params[param])

    def run(self, workflows, overwrite_params=None):

        self.logger.info(f"Checking for pending workflows...")

        if overwrite_params:
            self.logger.debug(f"Overwriting workflow parameters: {overwrite_params}")
            for workflow in workflows:
                self.overwrite_params(workflow, params=overwrite_params)

        for workflow in workflows:
            if self.should_run(workflow):
                self.logger.info(f"Running {workflow.name}...")
                workflow.run()
                self.logger.info(f"Finished running {workflow.name} with the status <{workflow.status}>")
                workflow.write_status_to_rds(workflow.name, workflow.owner_email, workflow.backup_email, workflow.status,
                                            workflow.run_time, env=self.env, error_value=workflow.error_value, error_type=workflow.error_type)
        else:
            self.logger.info(f"No pending workflows found")

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


def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
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
    if not logger:
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
