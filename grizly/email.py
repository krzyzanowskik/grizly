from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import Credentials, Account, Message, HTMLBody, Configuration, DELEGATE, FaultTolerance, HTMLBody
from grizly.utils import read_config
from grizly.orchestrate import retry
from grizly.config import (
    Config,
    _validate_config
)




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


    def __init__(self, subject, body, logger=None, is_html=False, email_address:str=None, email_password:str=None, config_key:str=None):
        self.subject = subject
        if is_html :
            self.body = HTMLBody(body)
        else:
            self.body = body
        self.logger = logger
        self.config_key = config_key if config_key else 'standard'
        if email_address is None:
            _validate_config(config=Config.data[config_key],
                            service='email')
            self.email_address = Config.data[config_key]['email']['email_address']
        else:
            self.email_address = email_address
        if email_password is None:
            _validate_config(config=Config.data[config_key],
                            service='email')
            self.email_password = Config.data[config_key]['email']['email_password']
        else:
            self.email_password = email_password


    @retry(Exception, tries=5, delay=5)
    def send(self, to, cc=None, send_as=None):

        to = to if isinstance(to, list) else [to]
        cc = cc if cc is None or isinstance(cc, list) else [cc]

        email_address = self.email_address
        email_password = self.email_password
        if send_as is None:
            _validate_config(config=Config.data[config_key],
                            service='email')
            send_as = Config.data[config_key]['email']['send_as']

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

        try:
            m.send()
        except Exception as e:
            if self.logger:
                self.logger.exception(f"Email not sent.")
            else:
                print(e)

        return None
