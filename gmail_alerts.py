import httplib2
import os
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apiclient import errors, discovery
from oauth2client import file, client, tools
SCOPES = 'https://www.googleapis.com/auth/gmail.send'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Gmail API Python Send Email'



class GmailAlerts:

    def __init__(self, sender, receiver):
        self.sender = sender
        self.receiver = receiver


    def get_credentials(self):
        store = file.Storage('token.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
            creds = tools.run_flow(flow, store)
        return creds

    def SendMessage(self,sender, to, subject, msgHtml, msgPlain):
        credentials = self.get_credentials()
        http = credentials.authorize(httplib2.Http())
        service = discovery.build('gmail', 'v1', http=http)
        message1 = self.CreateMessageHtml(sender, to, subject, msgHtml, msgPlain)
        result = self.SendMessageInternal(service, "me", message1)
        return result

    def SendMessageInternal(self,service, user_id, message):
        try:
            #msg = base64.urlsafe_b64encode(message)
            raw = message['raw']
            raw_decoded = raw.decode("utf-8")
            message = {'raw': raw_decoded}
            message = (service.users().messages().send(userId=user_id, body=message).execute())
            print( 'Message Id: %s' % message['id'])
            return message
        except Exception as error:
            print ('An error occurred: %s' % error)
            return "Error"
        return "OK"

    def CreateMessageHtml(self,sender, to, subject, msgHtml, msgPlain):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = to
        msg.attach(MIMEText(msgPlain, 'plain'))
        #msg.attach(MIMEText(msgHtml, 'html'))
        print(type(msg))
        print(msg.as_string())
        return {'raw': base64.urlsafe_b64encode(str.encode(msg.as_string()))}



if __name__ == "__main__":
    to = 'me@gmail.com, you@gmail.com'
    sender = "guru.vasuraj@gmail.com"
    subject = "subject"
    msgHtml = "Some message"
    msgPlain = "Some message"
    mailsys = GmailAlerts(sender,to)
    mailsys.SendMessage(sender, to, subject, msgHtml, msgPlain)




