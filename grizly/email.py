from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import Credentials, Account, Message, HTMLBody, Configuration, DELEGATE, FaultTolerance, HTMLBody, FileAttachment
from grizly.utils import read_config
from grizly.orchestrate import retry
from grizly.config import (
    Config,
    _validate_config
)
from os.path import basename



class Email:
    """Sends email using Exchange Web Services (EWS) API.

    Usage
    -------
    to = "test@example.com"
    cc = ["test2@example.com", test3@example.com"]
    team_email_address = "shared_mailbox@example.com"
    attachment_path = "path/to/attachment/myattachment.pdf"
    email = Email("test", "testing body")
    email.send(to=to, cc=cc, send_as=team_email_address)
    """


    def __init__(self, subject, body, attachment_path=None, logger=None, is_html=False, email_address:str=None, email_password:str=None, config_key:str=None):
        self.subject = subject
        self.body = body if not is_html else HTMLBody(body)
        self.logger = logger
        self.config_key = config_key if config_key else 'standard'
        if email_address is None:
            _validate_config(config=Config.data[self.config_key],
                            services='email')
            self.email_address = Config.data[self.config_key]['email']['email_address']
        else:
            self.email_address = email_address
        if email_password is None:
            _validate_config(config=Config.data[self.config_key],
                            services='email')
            self.email_password = Config.data[self.config_key]['email']['email_password']
        else:
            self.email_password = email_password
        self.attachment_path = attachment_path
        self.attachment_name = basename(attachment_path) if attachment_path else None
        self.attachment_content = self.get_attachment_content(attachment_path)
        self.attachment = self.get_attachment()

    def get_attachment(self):

        """ Returns FileAttachment object """

        if not self.attachment_path:
            return None

        return FileAttachment(name=self.attachment_name, content=self.attachment_content)

    def get_attachment_content(self, attachment_path):

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


    def send(self, to, cc=None, send_as=None):

        to = to if isinstance(to, list) else [to]
        cc = cc if cc is None or isinstance(cc, list) else [cc]

        email_address = self.email_address
        email_password = self.email_password
        if send_as is None:
            _validate_config(config=Config.data[self.config_key],
                            services='email')
            send_as = Config.data[self.config_key]['email']['send_as']

        if send_as == '':
            send_as = email_address

        BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter # change this in the future to avoid warnings
        credentials = Credentials(email_address, email_password)
        config = Configuration(server='smtp.office365.com', credentials=credentials, retry_policy=FaultTolerance(max_wait=60*5))
        account = Account(primary_smtp_address=send_as, credentials=credentials, config=config, autodiscover=False, access_type=DELEGATE)


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

        return None
