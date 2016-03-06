-- Create Schema

CREATE SCHEMA `bigbang` DEFAULT CHARACTER SET utf8 ;

-- Create Table Atendimento

Create Table If Not Exists atendimento (
    pkey Numeric,
    ID Text,
    nomeRegional Text,
    codigoRegiao Text,
    secretaria Text,
    codigoBairro Numeric,
    nomeBairro  Text,
    codigoAssunto Numeric,
    descricaoAssunto Text,
    anoSolicitacao Numeric,
    tipoSolicitacao Numeric,
    descricaoTipoSolicitacao  Text,
    statusSolicitacao Numeric,
    descricaoStatus  Text,
    dataCadastro Text,
    dataPrevisaoResposta Text,
    dataAtendimento Text,
    dataConclusao Text,
    cep  Text,
    tipoLogradouro Text,
    nomeLogradouro  Text,
    dataProvidencia Text,
    numeroSolicitacao Numeric,
    questionado tinyint
)

-- Create Table Despesa

Create Table If Not Exists despesa (
    pkey Numeric,
    ID Text,
    anoMesEmissao numeric,
    diaLancamento numeric,
    diaVencimento numeric,
    notaEmpenho Text,
    processoDescricao Text,
    valorEmpenho numeric,
    valorLiquidado numeric,
    valorALiquidar numeric,
    valorPago numeric,
    valorAPagar numeric,
    valorAcrescimoEmpenho numeric,
    valorAcrescimoLiquidado numeric,
    valorAcrescimoALiquidar numeric,
    valorAcrescimoPago numeric,
    valorAcrescimoAPagar numeric
)

-- Create Table sum_secretaria

Create Table If Not Exists summ_secretaria (
pkey Numeric,
secretaria text,
anosolicitacao Numeric,
mesSolicitacao Numeric,
qtdChamados Numeric,
qtdQuestionados Numeric,
min_cad_ate Numeric,
max_cad_ate Numeric,
med_cad_ate Numeric,
min_espera_prev Numeric,
max_espera_prev Numeric,
med_espera_prev Numeric,
min_diff_prev_prov Numeric,
max_diff_prev_prov Numeric,
med_diff_prev_prov Numeric,
foraprazo Numeric,
antesprazo Numeric,
maioratraso Numeric,
maiorantecipacao Numeric,
min_diff_cad_conc Numeric,
max_diff_cad_conc Numeric,
med_diff_cad_conc Numeric
)
;

ALTER TABLE `bigbang`.`atendimento`
CHANGE COLUMN `pkey` `pkey` BIGINT(20) NOT NULL AUTO_INCREMENT ;

ALTER TABLE `bigbang`.`summ_secretaria`
CHANGE COLUMN `pkey` `pkey` BIGINT(20) NOT NULL AUTO_INCREMENT ,
ADD PRIMARY KEY (`pkey`);

