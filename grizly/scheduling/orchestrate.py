import datetime as dt
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone, date
from functools import wraps
from json.decoder import JSONDecodeError
from logging import Logger
from time import sleep, time
from typing import Any, Dict, Iterable, List

import dask
import graphviz
import pandas as pd
from croniter import croniter
from dask.core import get_dependencies
from dask.dot import _get_display_cls
from dask.optimization import key_split
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from exchangelib.errors import ErrorFolderNotFound

from .config import Config

from .email import Email, EmailAccount
from exchangelib.errors import ErrorFolderNotFound

from .etl import df_to_s3, s3_to_rds
from .utils import read_config, get_path

LISTENER_STORE = get_path(
    "acoe_projects", "workflows_dev", "etc", "listener_store.json"
)


def cast_to_date(maybe_date: Any) -> dt.date:
    """
    Casts a date/datetime-like value to a Date object.

    Parameters
    ----------
    maybe_date : Any
        The date/datetime-like value to be conveted to a Date object.

    Returns
    -------
    _date
        The date-like object converted to an actual Date object.

    Raises
    ------
    TypeError
        When maybe_date is not of one of the supported types.
    """

    def _validate_date(maybe_date: Any) -> bool:
        if not isinstance(maybe_date, (dt.date, int)):
            return False
        return True

    if isinstance(maybe_date, str):
        _date = datetime.strptime(maybe_date, "%Y-%m-%d").date()
    elif isinstance(maybe_date, datetime):
        _date = datetime.date(maybe_date)
    else:
        _date = maybe_date

    if not _validate_date(_date):
        raise TypeError(f"The specified trigger field is not of type (int, date)")

    return _date


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

    def __init__(self, cron, name=None, start_date=None, end_date=None):
        if not croniter.is_valid(cron):
            raise ValueError("Invalid cron string: {}".format(cron))
        self.cron = cron
        self._name = name
        self.start_date = start_date
        self.end_date = end_date

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    def emit_dates(self, start_date: datetime = None) -> Iterable[datetime]:
        """
        Generator that emits workflow run dates
        Args:
            - start_date (datetime, optional): an optional schedule start date
        Returns:
            - Iterable[datetime]: the next scheduled dates
        """
        if not self.start_date:
            start_date = datetime.now(timezone.utc)

        cron = croniter(self.cron, start_date)

        while True:
            next_date = cron.get_next(datetime).replace(tzinfo=timezone.utc)
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

    def __repr__(self):
        return f"{type(self).__name__}({self.cron})"


class Trigger:
    def __init__(self, func, params=None):
        self.check = func
        self.kwargs = params or {}
        self.table = params.get("table")
        self.schema = params.get("schema")

    @property
    def last_table_refresh(self):
        return self.check(**self.kwargs)

    @property
    def should_run(self):
        return bool(self.last_table_refresh)


class Listener:
    """
    A class that listens for changes in a table and server as a trigger for upstream-dependent workflows.

    Checks and stores table's last refresh/trigger date.
    """

    def __init__(
        self,
        workflow,
        class_name="Listener",
        schema=None,
        table=None,
        field=None,
        query=None,
        db="denodo",
        trigger=None,
        trigger_type="default",
        delay=300,
    ):

        self.workflow = workflow
        self.name = workflow.name
        self.db = db
        self.schema = trigger.schema if trigger else schema
        self.table = table or trigger.table
        self.field = field
        self.query = query
        self.logger = logging.getLogger(__name__)
        self.trigger_type = trigger_type
        self.trigger = trigger
        self.last_data_refresh = self.get_last_json_refresh(key="last_data_refresh")
        self.engine = self.get_engine()
        self.last_trigger_run = self.get_last_json_refresh(key="last_trigger_run")
        self.delay = delay
        self.config_key = "standard"

    def __repr__(self):
        if self.query:
            return f'{type(self).__name__}(query="""{self.query}""")'
        return f"{type(self).__name__}(db={self.db}, schema={self.schema}, table={self.table}, field={self.field})"

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
                        msg = f"{e}, \nRetrying in {mdelay} seconds..."
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
        engine = create_engine(engine_str, encoding="utf8", poolclass=NullPool)

        return engine

    def get_last_json_refresh(self, key):
        with open(LISTENER_STORE) as f:
            listener_store = json.load(f)
            if not listener_store.get(self.name):
                return None
            last_json_refresh = listener_store[self.name].get(
                key
            )  # int or serialized date
            try:
                # attempt to convert the serialized datetime to a date object
                last_json_refresh = datetime.date(
                    datetime.strptime(last_json_refresh, r"%Y-%m-%d")
                )
            except:
                pass
            return last_json_refresh

    def update_json(self):
        with open(LISTENER_STORE) as json_file:
            try:
                listener_store = json.load(json_file)
            except JSONDecodeError:
                listener_store = {}
        if not isinstance(
            self, EmailListener
        ):  # to be done properly with TriggerListener subclass
            if self.trigger:
                listener_store[self.name] = {
                    "last_trigger_run": str(self.last_trigger_run)
                }
        else:
            if isinstance(self.last_data_refresh, dt.date):
                listener_store[self.name] = {
                    "last_data_refresh": str(self.last_data_refresh)
                }
            else:
                listener_store[self.name] = {
                    "last_data_refresh": self.last_data_refresh
                }

        with open(LISTENER_STORE, "w") as f_write:
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
            table_refresh_date = cursor.fetchone()[0]
        except TypeError:
            self.logger.exception(f"{self.name}'s trigger field is empty")
            raise

        cursor.close()
        con.close()

        # try casting to date
        table_refresh_date = cast_to_date(table_refresh_date)

        return table_refresh_date

    def should_trigger(self, table_refresh_date=None):

        if not isinstance(
            self, EmailListener
        ):  # to be done properly with TriggerListener subclass
            if self.trigger:
                today = datetime.today().date()
                if today == self.last_trigger_run:
                    return False  # workflow was already ran today
                return self.trigger.should_run

        self.logger.info(
            f"{self.name}: last data refresh: {self.last_data_refresh}, table_refresh_date: {table_refresh_date}"
        )
        # first run
        if not self.last_data_refresh:
            return True
        # the table ran previously, but now the refresh date is None
        # this means the table is being cleared in preparation for update
        if not table_refresh_date:
            return False
        # trigger on day change
        if table_refresh_date != self.last_data_refresh:
            return True

        # elif isinstance(self.trigger_type, Schedule):
        #     next_check_on = datetime.date(self.trigger_type.next_run)
        #     if next_check_on == table_refresh_date:
        #         return True

    def detect_change(self) -> bool:
        """Determine whether the listener should trigger a worfklow run.

        Returns
        -------
        bool
            Whether the field (specified by a Trigger function or the field parameter) has changed since it was last checked,
            as determined by Listener.should_trigger().

        Raises
        ------
        ValueError
            If neither of [field, query, trigger] is provided.
        """

        if not isinstance(
            self, EmailListener
        ):  # to be done properly with adding more subclasses
            if not any([self.field, self.query, self.trigger]):
                raise ValueError("Please specify the trigger for the listener")
            if self.trigger:
                if self.should_trigger():
                    today = datetime.today().date()
                    self.last_trigger_run = today
                    self.update_json()
                    self.logger.info(
                        f"Waiting {self.delay} seconds for the table upload to be finished before runnning workflow..."
                    )
                    sleep(self.delay)
                    return True
        try:
            table_refresh_date = self.get_table_refresh_date()
        except:
            if isinstance(self, EmailListener):
                self.logger.exception(
                    f"Could not retrieve refresh date from {self.search_email_address}'s {self.email_folder} folder"
                )
            else:
                self.logger.exception(
                    f"Connection or query error when connecting to {self.db}"
                )
            table_refresh_date = None

        if self.should_trigger(table_refresh_date):
            self.last_data_refresh = table_refresh_date
            self.update_json()
            self.logger.info(
                f"Waiting {self.delay} seconds for the table upload to be finished before runnning workflow..."
            )
            sleep(self.delay)
            return True

        return False


class EmailListener(Listener):
    def __init__(
        self,
        workflow,
        schema=None,
        table=None,
        field=None,
        query=None,
        db=None,
        trigger=None,
        trigger_type="default",
        delay=300,
        notification_title=None,
        email_folder=None,
        search_email_address=None,
        email_address=None,
        email_password=None,
        proxy=None,
    ):
        self.name = workflow.name
        self.notification_title = notification_title or workflow.name.lower().replace(
            " ", "_"
        )
        self.db = db
        self.logger = logging.getLogger(__name__)
        self.engine = None
        self.last_trigger_run = self.get_last_json_refresh(key="last_trigger_run")
        self.delay = delay
        self.config_key = "standard"
        self.email_folder = email_folder
        self.search_email_address = search_email_address
        self.email_address = email_address
        self.email_password = email_password
        self.proxy = proxy
        self.last_data_refresh = self.get_last_json_refresh(key="last_data_refresh")
        # self.last_data_refresh = self.get_last_email_date(
        #     self.workflow_report_name,
        #     self.email_folder,
        #     self.search_email_address,
        #     email_address=self.email_address,
        #     email_password=self.email_password,
        #     config_key=self.config_key,
        # )
        # super().__init__(self, self.last_data_refresh)

    def __repr__(self):
        return f"{type(self).__name__}(notification_title={self.notification_title}, search_email_address={self.search_email_address}, email_folder={self.email_folder})"

    def _validate_folder(self, account, folder_name):
        if not folder_name:
            return True
        try:
            folder = account.inbox / folder_name
        except ErrorFolderNotFound:
            raise

    def get_table_refresh_date(self):
        return self.get_last_email_date(
            self.notification_title,
            self.email_folder,
            self.search_email_address,
            self.email_address,
            self.email_password,
            self.config_key,
        )

    def get_last_email_date(
        self,
        notification_title,
        email_folder=None,
        search_email_address=None,
        email_address=None,
        email_password=None,
        config_key="standard",
    ):
        account = EmailAccount(
            email_address,
            email_password,
            alias=self.search_email_address,
            proxy=self.proxy,
        ).account
        self._validate_folder(account, email_folder)
        last_message = None

        if email_folder:
            try:
                last_message = (
                    account.inbox.glob("**/" + email_folder)
                    .filter(subject=notification_title)
                    .order_by("-datetime_received")
                    .only("datetime_received")[0]
                )
            except IndexError:
                self.logger.warning(
                    f"No notifications for {self.name} were found in {self.search_email_address}'s {email_folder} folder"
                )
        else:
            try:
                last_message = (
                    account.inbox.filter(subject=notification_title)
                    .filter(subject=notification_title)
                    .order_by("-datetime_received")
                    .only("datetime_received")[0]
                )
            except IndexError:
                self.logger.warning(
                    f"No notifications for {self.name} were found in Inbox folder"
                )

        if not last_message:
            return None

        d = last_message.datetime_received.date()
        last_received_date = date(d.year, d.month, d.day)

        return last_received_date


class TriggerListener(Listener):
    pass


class FieldListener(Listener):
    pass


class QueryListener(Listener):
    pass


class Workflow:
    """
    A class for running Dask tasks.
    """

    def __init__(
        self,
        name,
        owner_email,
        backup_email,
        tasks,
        trigger_on_success=None,
        execution_options: dict = None,
    ):
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

    # def add_task(fn, max_retries=5, retry_delay=5, depends_on=None):
    #     tasks = [load_qf(qf, depends_on=None)]
    #     fnretry = retry(fn)
    #     self.tasks
    #     return fn_decorated

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
                        msg = f"{e}, \nRetrying in {mdelay} seconds..."
                        logger.warning(msg)
                        sleep(mdelay)
                        mtries -= 1
                        mdelay *= backoff

                return f(*args, **kwargs)

            return f_retry  # true decorator

        return deco_retry

    @retry_task(Exception, tries=3, delay=300)
    def write_status_to_rds(
        self,
        name,
        owner_email,
        backup_email,
        status,
        run_time,
        env,
        error_value=None,
        error_type=None,
    ):

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
            "error_type": [error_type],
        }

        status_data = pd.DataFrame(status_data)

        try:
            df_to_s3(
                status_data,
                table,
                schema,
                if_exists="append",
                redshift_str="mssql+pyodbc://redshift_acoe",
                s3_key="bulk",
                bucket="acoe-s3",
            )
        except:
            self.logger.exception(f"{self.name} status could not be uploaded to S3")
            return None

        s3_to_rds(
            file_name=table + ".csv",
            schema=schema,
            if_exists="append",
            redshift_str="mssql+pyodbc://redshift_acoe",
            s3_key="bulk",
            bucket="acoe-s3",
        )

        self.logger.info(f"{self.name} status successfully uploaded to Redshift")

        return None

    def gen_scheduled_wf_email_body(self):
        run_time_str = str(timedelta(seconds=self.run_time))
        email_body = f"Scheduled workflow {self.name} has finished in {run_time_str} with the status {self.status}"
        return email_body

    def gen_triggered_wf_email_body(self):
        run_time_str = str(timedelta(seconds=self.run_time))
        l = self.listener
        if not isinstance(self.listener, EmailListener):
            if l.trigger:
                email_body = f"""Dependent workflow {self.name} has finished in {run_time_str} with the status {self.status}.
                    \nTrigger: {l.trigger.table} {l.field}'s latest value has changed to {l.trigger.last_table_refresh}"""
            else:
                email_body = f"""Dependent workflow {self.name} has finished in {run_time_str} with the status {self.status}.
                \nTrigger: {l.table} {l.field}'s latest value has changed to
                    {l.last_data_refresh}"""
        else:
            email_body = f"""Dependent workflow {self.name} has finished in {run_time_str} with the status {self.status}.
            \nTrigger:
            Received notification in {l.search_email_address}'s {l.email_folder} folder for
            {l.notification_title}'s update on {l.last_data_refresh}"""
        return email_body

    def gen_manual_wf_email_body(self):
        run_time_str = str(timedelta(seconds=self.run_time))
        email_body = f"Manual workflow {self.name} has finished in {run_time_str} with the status {self.status}"
        return email_body

    def add_stacktrace(self, email_body):
        email_body += f"\n\nError message: \n\n{self.error_message}"
        return email_body

    def generate_notification(self):
        # prepare email body; to be refactored into a function
        subject = f"Workflow {self.status}"
        if self.is_scheduled:
            email_body = self.gen_scheduled_wf_email_body()
        elif self.is_triggered:
            email_body = self.gen_triggered_wf_email_body()
        else:
            email_body = self.gen_manual_wf_email_body()
        if self.status == "fail":
            email_body += self.add_stacktrace(email_body)

        notification = Email(subject=subject, body=email_body, logger=self.logger)

        return notification

    def persist_start_time(self, time: float) -> None:
        if not os.path.exists("etc"):
            os.makedirs(f"etc")
        with open("etc/cur_wf_start_time.txt", "w+") as f:
            f.write(str(time))
        return None

    def run(self, env="local"):
        self.env = env
        start = time()
        self.persist_start_time(start)
        try:
            graph = dask.delayed()(self.tasks)
            if self.execution_options:
                scheduler = self.execution_options.get("scheduler") or self.scheduler
                num_workers = (
                    self.execution_options.get("num_workers") or self.num_workers
                )
            else:
                scheduler = "threads"
                num_workers = self.num_workers
            graph.compute(scheduler=scheduler, num_workers=num_workers)
            self.status = "success"
        except:
            exc_type, exc_value, exc_tb = sys.exc_info()
            self.error_value = (
                str(exc_value).replace("'", r"\'").replace('"', r"\"")[:250]
            )  # escape unintended delimiters
            self.error_type = str(exc_type).split("'")[
                1
            ]  # <class 'ZeroDivisionError'> -> ZeroDivisionError
            self.error_message = traceback.format_exc()
            self.logger.exception(f"{self.name} failed")
            self.status = "fail"
        end = time()
        self.run_time = int(end - start)

        notification = self.generate_notification()
        cc = (
            self.backup_email
            if isinstance(self.backup_email, list)
            else [self.backup_email]
        )
        to = (
            self.owner_email
            if isinstance(self.owner_email, list)
            else [self.owner_email]
        )
        send_as = ""

        notification.send(to=to, cc=cc, send_as=send_as)
        # when ran on server, the status is handled by Runner
        if env == "local":
            self.write_status_to_rds(
                self.name,
                self.owner_email,
                self.backup_email,
                self.status,
                self.run_time,
                env=self.env,
                error_value=self.error_value,
                error_type=self.error_type,
            )

        if self.trigger_on_success:
            triggered_wf = self.trigger_on_success
            self.logger.info(f"Running {triggered_wf.name}...")
            triggered_wf.run()
            self.logger.info(
                f"Finished running {triggered_wf.name} with the status <{triggered_wf.status}>"
            )
            triggered_wf.write_status_to_rds(
                triggered_wf.name,
                triggered_wf.owner_email,
                triggered_wf.backup_email,
                triggered_wf.status,
                triggered_wf.run_time,
                env=self.env,
                error_value=triggered_wf.error_value,
                error_type=triggered_wf.error_type,
            )

        return self.status


class Runner:
    """Workflow runner"""

    def __init__(self, logger: Logger = None, env: str = "prod") -> None:
        self.env = env
        self.logger = logging.getLogger(__name__)
        self.run_params = None

    def should_run(self, workflow: Workflow) -> bool:
        """Determines whether a workflow should run based on its next scheduled run.

        Parameters
        ----------
        workflow : Workflow
            A workflow instance. This function assumes the instance is generated on scheduler tick.
            Currently, this is accomplished by calling wf.generate_workflow() on each workflow every time
            Runner.get_pending_workflows() is called,
            which means wf.next_run recalculated every time should_run() is called

        Returns
        -------
        bool
            Whether the workflow's scheduled next run coincides with current time
        """

        if workflow.is_scheduled:

            self.logger.info(
                f"Determining whether scheduled workflow {workflow.name} shuld run... (next scheduled run: {workflow.next_run})"
            )

            now = datetime.now(timezone.utc)
            next_run = workflow.next_run

            if (next_run.day == now.day) and (
                next_run.hour == now.hour
            ):  # and (next_run.minute == now.minute): # minutes for precise scheduling
                workflow.next_run = workflow.schedule.next(1)[0]
                return True

        elif workflow.is_triggered:

            listener = workflow.listener

            if isinstance(listener, EmailListener):
                folder = listener.email_folder or "inbox"
                self.logger.info(
                    f"Listening for changes in {listener.search_email_address}'s {folder} folder"
                )
            else:
                self.logger.info(f"Listening for changes in {listener.table}...")

            if listener.detect_change():
                return True

        elif workflow.is_manual:
            return True

        return False

    # def get_pending_workflows(self, pending=None):

    #     pending = []
    #     for workflow in self.workflows:
    #         if self.should_run(workflow):
    #             pending.append(workflow)

    #     return pending

    def overwrite_params(self, workflow: Workflow, params: Dict) -> None:
        """Overwrites specified workflow's parameters

        Parameters
        ----------
        workflow : Workflow
            The workflow to modify.
        params : Dict
            Parameters and values which will be used to modify the Workflow instance.
        """
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

    def run(self, workflows: List[Workflow], overwrite_params: Dict = None) -> Dict:
        """[summary]

        Parameters
        ----------
        workflows : List[Workflow]
            Workflows to run.
        overwrite_params : Dict, optional
            Workflow parameters to overwrite (applies to all passed workflows), by default None

        Returns
        -------
        Dict
            Dictionary including each workflow together with its run status.
        """

        self.logger.info(f"Checking for pending workflows...")

        if overwrite_params:
            self.logger.debug(f"Overwriting workflow parameters: {overwrite_params}")
            for workflow in workflows:
                self.overwrite_params(workflow, params=overwrite_params)

        for workflow in workflows:
            if self.should_run(workflow):
                self.logger.info(f"Running {workflow.name}...")
                workflow.run(env=self.env)
                self.logger.info(
                    f"Finished running {workflow.name} with the status <{workflow.status}>"
                )
                workflow.write_status_to_rds(
                    workflow.name,
                    workflow.owner_email,
                    workflow.backup_email,
                    workflow.status,
                    workflow.run_time,
                    env=self.env,
                    error_value=workflow.error_value,
                    error_type=workflow.error_type,
                )
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

    def visualize(self, x, filename="simple_computation_graph", format=None):

        if hasattr(x, "dask"):
            dsk = x.__dask_optimize__(x.dask, x.__dask_keys__())
        else:
            dsk = x

        deps = {k: get_dependencies(dsk, k) for k in dsk}

        g = graphviz.Digraph(graph_attr={"rankdir": "LR"})

        nodes = set()
        edges = set()
        for k in dsk:
            key = self._node_key(k)
            if key not in nodes:
                g.node(key, label=key_split(k), shape="rectangle")
                nodes.add(key)
            for dep in deps[k]:
                dep_key = self._node_key(dep)
                if dep_key not in nodes:
                    g.node(dep_key, label=key_split(dep), shape="rectangle")
                    nodes.add(dep_key)
                # Avoid circular references
                if dep_key != key and (dep_key, key) not in edges:
                    g.edge(dep_key, key)
                    edges.add((dep_key, key))

        data = g.pipe(format=self.format)
        display_cls = _get_display_cls(self.format)

        if self.filename is None:
            return display_cls(data=data)

        full_filename = ".".join([filename, self.format])
        with open(full_filename, "wb") as f:
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
                    msg = f"{e}, \nRetrying in {mdelay} seconds..."
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
