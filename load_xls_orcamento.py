#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import xlrd
import mysql.connector
from mysql.connector import errorcode


def main():

    workbook = xlrd.open_workbook('orcamento_julho2015.xls')
    sheet = workbook.sheet_by_index(0)
    ano = u"{}".format(sheet.cell(1, 0).value).encode('UTF-8')
    row = 1
    totalSecretaria = {}

    while True:
        try:
            key = u"{}".format(sheet.cell(row, 3).value).encode('UTF-8')
            if key not in totalSecretaria:
                totalSecretaria[key] = 0

            totalSecretaria[key] += sheet.cell(row, 21).value

            row += 1
        except IndexError as e:
            break


    try:
        cnx = mysql.connector.connect(user='root', password = '', database='bigbang')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    cursor = cnx.cursor()
    cursor.execute( " delete from execucaoorcamentaria where ano = {}".format(ano))
    cursor.close()
    cursor = cnx.cursor()
    counter = 0
    for item in sorted(totalSecretaria):
        insertstmt = "insert into execucaoorcamentaria (Ano, secretaria, totaldisponivel) values ({}, '{}', {})".format(ano, item, totalSecretaria[item])
        cursor.execute(insertstmt)
        counter += 1
    print (str(counter) + " rows included to execucaoorcamentaria.")
    cnx.commit()
    cursor.close()
    cnx.close()

    return 0
# Run application
if __name__ == "__main__":
    sys.exit(main())
