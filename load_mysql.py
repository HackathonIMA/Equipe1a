#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import mysql.connector
from mysql.connector import errorcode
import requests
import unicodedata
import datetime

def getAndLoadAtendimento(conn):
    counter = 0
    cursor = conn.cursor()
    offset = 0
    hasrows = False
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    filters = "dataCadastro:{}".format(yesterday.strftime( '%d/%m/%Y'))

    cursor.execute("select 'haverows' from atendimento where dataCadastro = '{}' limit 1".format(yesterday.strftime( '%d/%m/%Y')))

    for item in cursor:
        hasrows = True

    if not hasrows:
        while True:
            #print ("offset: " + str(offset))
            r = requests.get('http://api.ima.sp.gov.br/v1/atendimento', headers={'client_id' : 'Jxd69cUSR2oL'}, params= {'limit': 99, 'offset': offset, 'filters':filters})

            if r.status_code == 200:
                results = r.json()
                for item in results:
                    counter += 1

                    atendimentoinsert = u"insert into atendimento (ID, nomeRegional, codigoRegiao, secretaria, codigoBairro, nomeBairro, codigoAssunto, descricaoAssunto, anoSolicitacao, tipoSolicitacao, descricaoTipoSolicitacao, statusSolicitacao,  descricaoStatus, dataCadastro, dataPrevisaoResposta, dataAtendimento, dataConclusao, cep, tipoLogradouro, nomeLogradouro, dataProvidencia, numeroSolicitacao, questionado) values ('{}', '{}', {}, '{}', {}, '{}', {}, '{}', {}, {}, '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', {}, {})".format(item['id'], item['nomeRegional'], item['codigoRegiao'], item['secretaria'], item['codigoBairro'], item['nomeBairro'], item['codigoAssunto'], item['descricaoAssunto'], item['anoSolicitacao'], item['tipoSolicitacao'], item['descricaoTipoSolicitacao'], item['statusSolicitacao'], item['descricaoStatus'], item['dataCadastro'], item['dataPrevisaoResposta'], item['dataAtendimento'], item['dataConclusao'], item['cep'], item['tipoLogradouro'], item['nomeLogradouro'], item['dataProvidencia'], item['numeroSolicitacao'], 0).encode('UTF-8')
                    try:
                        cursor.execute(atendimentoinsert)
                    except mysql.connector.Error as err:
                        print(err)
                        print (atendimentoinsert)
                print (counter)
            else:
                break
            if len(results) < 99:
                break
            offset += len(results)
        conn.commit()
        print (str(counter) + " rows included to atendimento.")
    cursor.close()

def createSummAtendimento(conn, month, year):
    deleteCursor = conn.cursor()
    insertCursor = conn.cursor()
    cursor = conn.cursor(buffered=True)

    deletestmt = u"delete from summ_secretaria where anoSolicitacao = {} and mesSolicitacao = {}".format(year, month).encode('UTF-8')
    deleteCursor.execute(deletestmt)
    deleteCursor.close()
    conn.commit()
    sumQuery = u'''
        SELECT
            secretaria,
            IfNull(anosolicitacao, 0),
            IfNull(mesSolicitacao, 0),
            count(1) qtdChamados,
            sum(questionado) qtdQuestionados,
            IfNull(MIN(tempo_ate_atender), 0) min_cad_ate,
            IfNull(MAX(tempo_ate_atender), 0) max_cad_ate,
            IfNull(AVG(tempo_ate_atender), 0) med_cad_ate,
            IfNull(MIN(tempo_previsto), 0) min_espera_prev,
            IfNull(MAX(tempo_previsto), 0) max_espera_prev,
            IfNull(AVG(tempo_previsto), 0) med_espera_prev,
            IfNull(MIN(diff_prev_prov), 0) min_diff_prev_prov,
            IfNull(MAX(diff_prev_prov), 0) max_diff_prev_prov,
            IfNull(AVG(diff_prev_prov), 0) med_diff_prev_prov,
            sum(if(tempo_prov_a_conclusao < 0, 1, 0)) foraprazo,
            sum(if(tempo_prov_a_conclusao > 0, 1, 0)) antesprazo,
            abs(IfNull(MIN(tempo_prov_a_conclusao), 0)) maioratraso,
            IfNull(MAX(tempo_prov_a_conclusao), 0) maiorantecipacao,
            IfNull(MIN(tempo_desde_atendimento), 0) min_diff_cad_conc,
            IfNull(MAX(tempo_desde_atendimento), 0) max_diff_cad_conc,
            IfNull(AVG(tempo_desde_atendimento), 0) med_diff_cad_conc

        FROM
            (SELECT
                a.numeroSolicitacao,
                    a.secretaria,
                    a.descricaoTipoSolicitacao,
                    anosolicitacao,
                    month(STR_TO_DATE(a.dataCadastro, '%d/%m/%Y')) mesSolicitacao,
                    questionado,
                    datediff(STR_TO_DATE(a.dataAtendimento, '%d/%m/%Y'), STR_TO_DATE(a.dataCadastro, '%d/%m/%Y')) tempo_ate_atender,
                    datediff(STR_TO_DATE(a.dataPrevisaoResposta, '%d/%m/%Y'), STR_TO_DATE(a.dataAtendimento, '%d/%m/%Y')) tempo_previsto,
                    datediff(STR_TO_DATE(a.dataProvidencia, '%d/%m/%Y'), STR_TO_DATE(a.dataPrevisaoResposta, '%d/%m/%Y')) diff_prev_prov,
                    datediff(STR_TO_DATE(a.dataConclusao, '%d/%m/%Y'), STR_TO_DATE(a.dataProvidencia, '%d/%m/%Y')) tempo_prov_a_conclusao,
                    datediff(STR_TO_DATE(a.dataConclusao, '%d/%m/%Y'), STR_TO_DATE(a.dataAtendimento, '%d/%m/%Y')) tempo_desde_atendimento
            FROM
                atendimento a
            WHERE
                anosolicitacao = {} and {} = month(STR_TO_DATE(a.dataCadastro, '%d/%m/%Y'))
        ) a
        GROUP BY secretaria, anosolicitacao, mesSolicitacao
        order by secretaria, anoSolicitacao, mesSolicitacao;
    '''.format(year, month).encode('UTF-8')

    cursor.execute(sumQuery)

    counter = 0
    for item in cursor:
        counter += 1
        sumsecretariaInsert = u"insert into summ_secretaria (secretaria, anosolicitacao, mesSolicitacao, qtdChamados, qtdQuestionados, min_cad_ate, max_cad_ate, med_cad_ate, min_espera_prev, max_espera_prev, med_espera_prev, min_diff_prev_prov, max_diff_prev_prov, med_diff_prev_prov, foraprazo, antesprazo, maioratraso, maiorantecipacao, min_diff_cad_conc, max_diff_cad_conc, med_diff_cad_conc) values ('{}', {}, {} ,{} ,{}, {}, {}, {} ,{} ,{}, {}, {}, {} ,{} ,{}, {}, {}, {} ,{} ,{}, {})".format(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[9], item[10], item[11], item[12], item[13], item[14], item[15], item[16], item[17], item[18], item[19], item[20]).encode('UTF-8')
        try:
            insertCursor.execute(sumsecretariaInsert)
        except mysql.connector.Error as err:
            print(err)
            print(sumsecretariaInsert)


    conn.commit()
    insertCursor.close()
    cursor.close()
    print (str(counter) + " rows included to summ_secretaria.")

def main():

    try:
      cnx = mysql.connector.connect(host='52.7.200.222', user='app', password = 'app', database='bigbang')
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)

    month = datetime.datetime.now().strftime("%m")
    year = datetime.datetime.now().strftime("%Y")
    getAndLoadAtendimento(cnx)
    createSummAtendimento(cnx, month, year)



    cnx.close()

    return 0

# Run application
if __name__ == "__main__":
    sys.exit(main())

