#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import mysql.connector
from mysql.connector import errorcode
import requests
import unicodedata
import datetime

def main():

    offset = 0
    filters = {}
    arraylinks = []

    try:
      cnx = mysql.connector.connect(user='root', password = '', database='bigbang')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    while True:
        #print ("offset: " + str(offset))

        r = requests.get('http://api.ima.sp.gov.br/v1/transparencia/unidadesGestoras', headers={'client_id' : 'Jxd69cUSR2oL'}, params= {'limit': 99, 'offset': offset, 'filters':filters})

        if r.status_code == 200:
            results = r.json()
            for item in results:
                arraylinks.append([item['descricao'], (item['links'][1]['href']).replace('/api', 'http://api.ima.sp.gov.br')])
        else:
            results = 0
            print r.status_code

        if len(results) < 99:
            break

        offset += len(results)

    offset = 0
    cursor = cnx.cursor()
    for item in arraylinks:
        r = requests.get(item[1], headers={'client_id' : 'Jxd69cUSR2oL'}, params= {'limit': 99, 'offset': offset, 'filters':filters})
        if r.status_code == 200:
            results = r.json()
            if len(results) > 0:
                insertstmt = u"insert into despesa (Descricao, ID, anoMesEmissao, diaLancamento, diaVencimento, notaEmpenho, processoDescricao, valorEmpenho, valorLiquidado, valorALiquidar, valorPago, valorAPagar, valorAcrescimoEmpenho, valorAcrescimoLiquidado, valorAcrescimoALiquidar, valorAcrescimoPago, valorAcrescimoAPagar) values ('{}', '{}', {}, {}, {}, '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format( (u"{}".format(item[0])), results[0]['ID'], results[0]['anoMes'], results[0]['diaLancamento'], results[0]['diaVencimento'], results[0]['notaEmpenho'], results[0]['processoDescricao'], results[0]['valorEmpenho'], results[0]['valorLiquidado'], results[0]['valorALiquidar'], results[0]['valorPago'], results[0]['valorAPagar'], results[0]['valorAcrescrimoEmpenho'], results[0]['valorAcrescimoLiquidado'], results[0]['valorAcrescimoALiquidar'], results[0]['valorAcrescimoPago'],results[0]['valorAcrescimoAPagar']).encode('UTF-8').replace('None', 'Null').replace(", ,", ", Null," )
                # print insertstmt
                cursor.execute(insertstmt)
            # for item in results:
            #     print(item)

        else:
            results = 0
            print r.status_code

    cursor.close()
    cnx.commit()
    cnx.close()
    return 0

# Run application
if __name__ == "__main__":
    sys.exit(main())

