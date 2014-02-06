#!/usr/bin/python3
__author__ = 'Ahmet Erkan ÇELİK'

import psycopg2
import argparse
from metafactory import metafactory

parser = argparse.ArgumentParser()

parser.add_argument("-host", "--hostname", help="Hostname", dest="hostname")
parser.add_argument("-db", "--database", help="Database name", dest="database")
parser.add_argument("-user", "--username", help="User name", dest="username")
parser.add_argument("-pass", "--password", help="Password", dest="password")
parser.add_argument("-sch", "--schemaname", help="Schema name", dest="schemaname", default="public")
parser.add_argument("-file", "--filename", help="Output file name", dest="file")

args = parser.parse_args()



try:
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s'" % (args.database, args.username, args.hostname, args.password))
except:
    print("I am unable to connect to the database")
    quit()

cur = conn.cursor()
file = open(args.file, "w", encoding='utf-8')
file.write("""from sqlalchemy import *
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgres import *
import json

Base = declarative_base()


""")
file.write(metafactory.tables(cur))
file.close()
