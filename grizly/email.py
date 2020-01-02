from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import Credentials, Account, Message, HTMLBody, Configuration, DELEGATE, FaultTolerance, HTMLBody, FileAttachment
from .utils import read_config, get_path
from .config import (
    Config,
    _validate_config
)
from os.path import basename
import os


class Email:
    """Class used to build and send email using Exchange Web Services (EWS) API.

    Parameters
    ----------
    subject : str
        Email subject
    body : str
        Email body
    attachment_path : str, optional
        Path to local file to be attached in the email , by default None
    logger : [type], optional
        [description], by default None
    is_html : bool, optional
        [description], by default False
    email_address : str, optional
        Email address used to send an email, by default None
    email_password : str, optional
        Password to the email sepcified in email_address, by default None
    config_key : str, optional
        Config key, by default 'standard'"""
    def __init__(self, subject:str, body:str, attachment_path:str=None, logger=None, is_html:bool=False
                    , email_address:str=None, email_password:str=None, config_key:str=None, proxy: str=None):
        self.subject = subject
        self.body = body if not is_html else HTMLBody(body)
        self.logger = logger
        self.config_key = config_key or "standard"
        if None in [email_address, email_password]:
            config = Config().get_service(config_key=self.config_key, service="email")
        self.email_address = email_address or config["email_address"]
        self.email_password = email_password or config["email_password"]
        self.attachment_path = attachment_path
        self.attachment_name = basename(attachment_path) if attachment_path else None
        self.attachment_content = self.get_attachment_content(attachment_path)
        self.attachment = self.get_attachment()
        try:
            self.proxy = proxy or os.getenv("HTTPS_PROXY") or Config().get_service(config_key=self.config_key, service="proxies").get("https")
        except:
            self.proxy = None


    def get_attachment(self):
        """ Returns FileAttachment object """

        if not self.attachment_path:
            return None

        return FileAttachment(name=self.attachment_name, content=self.attachment_content)


    def get_attachment_content(self, attachment_path:str):
        """ Get the content of a file in binary format """

        if not self.attachment_path:
            return None

        attachment_name = self.attachment_name

        image_formats = ["png", "jpeg", "jpg", "gif", "psd", "tiff"]
        doc_formats = ["pdf", "ppt", "pptx", "xls", "xlsx", "doc", "docx"]
        archive_formats = ["zip", "7z", "tar", "rar", "iso"]
        compression_formats = ["pkl", "gzip", "bz", "bz2"]

        binary_formats = image_formats + doc_formats + archive_formats + compression_formats
        text_formats = ["txt", "log", "html", "xml", "json", "py", "md", "ini", "yaml", "yml", "toml", "cfg", "csv", "tsv"]

        attachment_format = attachment_name.split(".")[-1]

        if attachment_format in binary_formats:
            with open(attachment_path, "rb") as f:
                binary_content = f.read()

        elif attachment_format in text_formats:
            with open(attachment_path) as f:
                text_content = f.read()
                binary_content = text_content.encode("utf-8")

        else:
            raise NotImplementedError(f"Attaching files with {attachment_format} type is not yet supported.\n"
                                      f"Try putting the file inside an archive.")

        return binary_content


    def send(self, to:list, cc:list=None, send_as:str=None):
        """Sends an email

        Parameters
        ----------
        to : str or list
            Email recepients
        cc : str or list, optional
            Cc recepients, by default None
        send_as : str, optional
            Author of the email, by default None

        Examples
        --------
        >>> personal = {
        ...        "personal": {
        ...        "email": {
        ...            "email_address": "john_snow@example.com",
        ...            "email_password": "wolf123",
        ...            "send_as": "John Snow"
        ...        }
        ...        }
        ...    }
        >>> conf = Config().add_keys(personal)
        Key 'personal' has been added.
        >>> attachment_path = get_path("dev", "grizly", "tests", "output.txt")
        >>> email = Email(subject="Test", body="Testing body.", attachment_path=attachment_path, config_key="personal")
        >>> to = "test@example.com"
        >>> cc = ["test2@example.com", "test3@example.com"]
        >>> team_email_address = "shared_mailbox@example.com"
        >>> #email.send(to=to, cc=cc, send_as=team_email_address) #uncomment this line to send an email

        Returns
        -------
        None
        """
        to = to if isinstance(to, list) else [to]
        cc = cc if cc is None or isinstance(cc, list) else [cc]

        if not send_as:
            try:
                send_as = Config().get_service(config_key=self.config_key, service="email").get('send_as')
            except KeyError:
                pass

        if send_as == "": # it's added as empty string in config
            send_as = self.email_address

        email_address = self.email_address
        email_password = self.email_password

        if self.proxy:
            os.environ["HTTPS_PROXY"] = self.proxy
        BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter # change this in the future to avoid warnings
        credentials = Credentials(email_address, email_password)
        config = Configuration(server="smtp.office365.com", credentials=credentials, retry_policy=FaultTolerance(max_wait=2*60))
        try:
            account = Account(primary_smtp_address=send_as, credentials=credentials, config=config, autodiscover=False, access_type=DELEGATE)
        except:
            raise ConnectionError("Connection to Exchange server failed. Please check your credentials and/or proxy settings")
        m = Message(
            account=account,
            subject=self.subject,
            body=self.body,
            to_recipients=to,
            cc_recipients=cc,
            author=send_as
        )

        if self.attachment:
            m.attach(self.attachment)

        try:
            m.send()
        except Exception as e:
            if self.logger:
                self.logger.exception(f"Email not sent.")
            else:
                print(e)
            raise

        return None
