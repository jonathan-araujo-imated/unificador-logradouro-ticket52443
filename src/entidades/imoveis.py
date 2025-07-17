import json, requests, os
from tqdm import tqdm
from src import _logger as log, loader, db_manager
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()


def job(ids_logradouros_filtro, id_logradouro_novo):
    log.info("Iniciando o job de atualização de imóveis->logradouros.")
        
    if not ids_logradouros_filtro:
        log.error("ids_logradouros_filtro vazio.")
        return
    if not id_logradouro_novo:
        log.error("id_logradouro_novo vazio.")
        return
    
    for logradouro in ids_logradouros_filtro:

        imoveis = get_imoveis_filtro(logradouro)
        if not imoveis:
            log.error(f"Sem registros de imoveis para o ID_LOGRADOURO: {logradouro}.")
            continue

        for imovel in tqdm(imoveis, desc=f"Atualizando imóveis [Logradouro -> {logradouro}]"):
            id_imovel = imovel["id_imovel"]
            getrow = getrows_imovel(id_imovel)
            if getrow:
                log.info(f"ID_IMOVEL: {id_imovel}, já processado.")
                continue

            log.info(f"Processando ID_IMOVEL ->{id_imovel}")
            imovel_dados = get_imovel(id_imovel)

            if not imovel_dados:
                log.error("Falha ao obter o GET do imóvel.")
                return
            
            put_imovel(imovel_dados, id_logradouro_novo)


def get_imoveis_filtro(id_logradouro):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/imoveis"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    limit = 100
    offset = 0
    todos_imoveis = []
    has_next = True

    while has_next:
        params_ = {
            "filter": f"(logradouro.id in ({id_logradouro}))",
            "limit": limit,
            "offset": offset
        }

        try:
            response = requests.get(url_, params=params_, headers=headers_)
        except (Exception, requests.exceptions.RequestException) as e:
            log.error("-> " + str(e))
            break

        if response.status_code != 200:
            log.error(f"-> {response.status_code} {response.text}")
            break

        retorno = response.json()

        itens = retorno["content"]
        ids_imoveis = [{"id_imovel": item["id"], "id_logradouro": id_logradouro} for item in itens if "id" in item]
        todos_imoveis.extend(ids_imoveis)
        # log.info(f"-> Response text: {ids_imoveis}")

        has_next = retorno.get("hasNext", False)
        offset += limit

    log.info(f"-> Total de imóveis filtrados: {len(todos_imoveis)}")
    
    return todos_imoveis


def get_imovel(id_imovel):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/imoveis/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.get(url_ + str(id_imovel), headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        return

    retorno = response.json()
    
    id_imovel_ret = retorno["id"]
    codigo_imovel_ret = retorno["codigo"]
    id_logradouro_ret = retorno["logradouro"].get("id")
    dados_json = json.dumps(retorno, ensure_ascii=False)

    ret_imovel = {"id_imovel": id_imovel_ret, 
                  "codigo_imovel": codigo_imovel_ret,
                  "id_logradouro": id_logradouro_ret, 
                  "dados_json": dados_json} 
    
    # log.info(f"-> Response text: {ret_imovel}")

    return ret_imovel

def inserir_imovel_dados(imovel):
    if not imovel:
        log.info("Nenhum imóvel para inserir.")
        return

    tabela_nome = "imoveis_logradouro_dados"
    colunas = [
        'id_imovel',
        'codigo_imovel',
        'situacao',
        'dados_json',
    ]
    sql_statements = loader.generate_insert_data_sql(tabela_nome, colunas, [imovel])
    
    if not sql_statements:
        log.info("Nenhum SQL gerado para inserção.")
        return

    loader.execute_sql_statements(sql_statements)
    log.info(f"Imovel inserido: {imovel["id_imovel"]}")  


def getrows_imovel(id_imovel):
    try:
        with db_manager.connect() as cnx:
            with cnx.cursor() as c:
                c.execute(
                    f"""
                    select
                        id_imovel,
                        codigo_imovel,
                        situacao
                    from imoveis_logradouro_dados
                    where id_imovel = '{id_imovel}'
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None


def definir_body_put(imovel, id_logradouro):
    # data_imovel = getrows_imovel(id_imovel)

    if not imovel:
        log.error("Nenhum resultado de 'imoveis_logradouro_dados' .")
        return
    
    if not id_logradouro:
        log.error("Nenhum id_logradouro informado.")
        return

    dados_json = imovel["dados_json"]
    if isinstance(dados_json, str):
        try:
            dados_json = json.loads(dados_json)
        except json.JSONDecodeError as e:
            print("Erro ao fazer json.loads em dados_json:", e)
            dados_json = {}

    dados_json["logradouro"] = {"id": id_logradouro}
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    dados_json["alteracaoDetalhes"] = {
                                        "dtVigencia": data_hoje,
                                        "nroProcesso": f"{os.getenv("TICKET_LOG_ALTER")}",
                                        "observacoes": "Processo de Unificação de Logradouros"
                                       }

    dados_json_str = json.dumps(dados_json, ensure_ascii=False)
    body = dados_json_str

    return body


def put_imovel(imovel, id_logradouro):
    id_imovel = imovel["id_imovel"]
    if not id_imovel:
        log.error("PUT -> id_imovel não encontrado.")
        return
    
    body = definir_body_put(imovel, id_logradouro)
    if not body:
        log.info("Nenhum body 'definir_body_put()'.")
        return
    
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/imoveis/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.put(url_ + str(id_imovel), data=body ,headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        imovel["situacao"] = f"ERRO -> {response.status_code} {response.text}"
        inserir_imovel_dados(imovel)
        return

    imovel["situacao"] = "SUCESSO"
    inserir_imovel_dados(imovel)
    retorno = response.json()
    
    id_imovel_ret = retorno["id"]
    logradouro_ret = retorno["logradouro"]

    ret_imovel = {"id_imovel": id_imovel_ret, "logradouro": logradouro_ret} 
    
    log.info(f"-> PUT Response text: {ret_imovel}")

    return ret_imovel

