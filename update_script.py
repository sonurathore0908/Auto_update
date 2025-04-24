import pandas as pd
import json
import redshift_connector
import smtplib
from datetime import date, datetime, timedelta
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
with open('config.json', 'r') as f:
    config = json.load(f)

def query_generator(packages_name,schema_table,columns,condition,select_columns):
    select_query=f"select count(*) from {schema_table} where {condition} ;"
    update_query=f"update {schema_table} set {columns} where  {condition} ; "
    return select_query,update_query

def colunms(columns):
    set_clause = ", ".join(f"{col} = {val}" for col, val in columns.items())
    select_clause = ", ".join(f"{col} " for col in columns.keys())
    return set_clause,select_clause

def send_mail(select_counts,update_counts,package_name,select_query,update_query,data_type):

    html=f"""\
    <!DOCTYPE html>
                    <html>
                    <head lang="en">
                        <meta charset="UTF-8">
                        <title>zee5 report</title>
                    </head>
                    <body>

                            <h4>Hi Customer Operation team, </h4>
                            <br><center>
							<B>{select_query}<br>
							update Status of {data_type }{package_name}.<br>

                    <B>select_counts:- {select_counts}</a><br>
					<br><br>
					{update_query}<br>
					Updates:- counts:- {update_counts}<br></B>



                </center>
                                <B></B>
                                <br><br><br>
                            </p>
                            <p> <br>

                            </p>


                        <br><br><B>Thanks</B><br>
                        <B>Sonu Rathore</p>
                    </body>
                    </html>
    """
    host = "email-smtp.amazonaws.com"
    port = 587
    username = "************"
    password = "***************"
    from_email = "<mail_id.com>"


    emails = "<sonu.rathore@mfilterit.com>"
    emails_to=["sonu.rathore@mfilterit.com"]
    emails_cc=["3rd_mail_id.com","2nd_mail_id.com","1st_mail_id.com"]

    msg = MIMEMultipart()
    msg['From'] = "<report@mfilterit.com>"
    msg['To'] = ",".join(emails_to)
    msg['Cc']= ",".join(emails_cc)
    msg['Subject'] = f"Updates status of {data_type} {package_name}."
    part1=MIMEText(html,'html')
    msg.attach(part1)
    server = smtplib.SMTP(host, port)
    server.starttls()
    server.login(username, password)
    server.sendmail(msg['From'], emails, msg.as_string())
    server.quit()
    print("successfully sent email %s:" % (msg['To']))
    print( "successfully sent email to %s:" % (msg['Cc']))



def update_func(update_query, select_query):

    try:
        conn = redshift_connector.connect(
            host="mf-app-reporting-cluster.c5vbshpmm9v5.us-west-2.redshift.amazonaws.com",
            user="user_name",
            password="password",
            port=30016,
            database="database_name"
        )
        
        print("✅ Connection successful!")

        cursor = conn.cursor()
        cursor.execute(select_query)
        count_select = cursor.fetchall()

        cursor.execute(update_query)
        update_counts = cursor.rowcount  
        conn.commit()
        return update_counts, count_select

    except Exception as e:
        print(f"❌ Error: {e}")
        return 0, 0, [], []  

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        print("Connection closed.")

for i in config['update']:
    packages_name=i['id']
    schema_table=i['schema_table']
    columns=i['columns']
    condition=i['condition']
    data_type=i['data_type']
    set_columns,select_columns=colunms(columns)
    select_query,update_query=query_generator(packages_name,schema_table,set_columns,condition,select_columns)
    print(select_query)
    print('\n\n',update_query)
    update_count,select_count=update_func(update_query,select_query)
    send_mail(select_count,update_count,packages_name,select_query,update_query,data_type)
