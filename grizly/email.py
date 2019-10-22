from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import Credentials, Account, Message, HTMLBody, Configuration, DELEGATE, FaultTolerance
from grizly.utils import read_config
from grizly.orchestrate import retry

config = read_config()


class Email:
    """Sends email using Exchange Web Services (EWS) API.

    Usage
    -------
    to = "test@example.com"
    cc = ["test2@example.com", test3@example.com"]
    team_email_address = "shared_mailbox@example.com"
    email = Email("test", "testing body")
    email.send(to=to, cc=cc, send_as=team_email_address)
    """


    def __init__(self, subject, body, logger=None):
        self.subject = subject
        self.body = body
        self.logger = logger
        self.email_address = config["email_address"]
        self.email_password = config["email_password"]


    @retry(Exception, tries=5, delay=5)
    def send(self, to, cc=None, send_as=None):

        to = to if isinstance(to, list) else [to]
        cc = cc if cc is None or isinstance(cc, list) else [cc]

        email_address = self.email_address
        email_password = self.email_password
        if not send_as:
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

        try:
            m.send()
        except Exception as e:
            if self.logger:
                self.logger.exception(f"Email not sent.")
            else:
                print(e)

        return None
