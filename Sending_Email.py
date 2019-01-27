import smtplib

port = "465"
password = input("Password> ")
smtpgmail = "smtp.gmail.com"

server = smtplib.SMTP_SSL()
server.connect(smtpgmail, port)
server.ehlo()
server.login("zsomborjoeldev@gmail.hu", password)
print("Logged in.")

#Email sending info
FROM = "zsomborjoeldev@gmail.hu"
TO = "someone@gmail.com".split()
MESSAGE = """\
Subject: Hiya!

This is an email sent from python script.
""".encode('utf-8')

server.sendmail(FROM, TO, MESSAGE)
print("Email sent.")
