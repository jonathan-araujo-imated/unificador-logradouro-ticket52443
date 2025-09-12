import json, requests, os
from tqdm import tqdm
from src import _logger as log, loader, db_manager
from dotenv import load_dotenv


load_dotenv()


def job(ids_logradouros_filtro, id_logradouro_novo):
    log.info("Iniciando o job de atualização de Econômicos -> Logradouros.")
        
    if not ids_logradouros_filtro:
        log.error("ids_logradouros_filtro vazio.")
        return
    if not id_logradouro_novo:
        log.error("id_logradouro_novo vazio.")
        return
    
    for logradouro in ids_logradouros_filtro:

        economicos = get_economicos_filtro(logradouro)
        if not economicos:
            log.error(f"Sem registros de economicos para o ID_LOGRADOURO: {logradouro}.")
            continue

        for economico in tqdm(economicos, desc=f"Atualizando econômicos [Logradouro -> {logradouro}]"):
            id_economico = economico["id_economico"]
            getrow = getrows_economico(id_economico)
            if getrow:
                log.info(f"ID_ECONOMICO: {id_economico}, já processado.")
                continue

            log.info(f"Processando ID_ECONOMICO ->{id_economico}")
            economico_dados = get_economico(id_economico)
            
            put_economico(economico_dados, id_logradouro_novo)


def get_economicos_filtro(id_logradouro):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/economicos"
           

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    limit = 100
    offset = 0
    todos_economicos = []
    has_next = True

    while has_next:
        params_ = {
            "filter": f"((municipio=2373 and logradouro={id_logradouro}))",
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
        ids_economicos = [{"id_economico": item["id"], "id_logradouro": id_logradouro} for item in itens if "id" in item]
        todos_economicos.extend(ids_economicos)

        has_next = retorno.get("hasNext", False)
        offset += limit

    log.info(f"-> Total de Econômicos filtrados: {len(todos_economicos)}")
    
    return todos_economicos


def get_economico(id_economico):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/economicos/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.get(url_ + str(id_economico), headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        return

    retorno = response.json()
    
    id_economico_ret = retorno["id"]
    codigo_economico_ret = retorno["codigo"]
    id_logradouro_ret = retorno["logradouro"].get("id")
    dados_json = json.dumps(retorno, ensure_ascii=False)
    # dados_json = retorno

    ret_economico = {"id_economico": id_economico_ret, 
                  "codigo_economico": codigo_economico_ret,
                  "id_logradouro": id_logradouro_ret, 
                  "dados_json": dados_json} 
    
    return ret_economico

def inserir_economico_dados(economico):
    if not economico:
        log.info("Nenhum econômico para inserir.")
        return

    tabela_nome = "economicos_logradouro_dados"
    colunas = [
        'id_economico',
        'codigo_economico',
        'situacao',
        'dados_json',
    ]
    sql_statements = loader.generate_insert_data_sql(tabela_nome, colunas, [economico])
    
    if not sql_statements:
        log.info("Nenhum SQL gerado para inserção.")
        return

    loader.execute_sql_statements(sql_statements)
    log.info(f"Econômico inserido: {economico["id_economico"]}")  


def getrows_economico(id_economico):
    try:
        with db_manager.connect() as cnx:
            with cnx.cursor() as c:
                c.execute(
                    f"""
                    select
                        id_economico,
                        codigo_economico,
                        situacao
                    from economicos_logradouro_dados
                    where id_economico = '{id_economico}'
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None


def definir_body_put(economico, id_logradouro):

    if not economico:
        log.info("Nenhum resultado de 'economicos_dados' .")
        return
    
    if not id_logradouro:
        log.error("Nenhum id_logradouro informado.")
        return

    dados_json = economico["dados_json"]
    if isinstance(dados_json, str):
        try:
            dados_json = json.loads(dados_json)
        except json.JSONDecodeError as e:
            print("Erro ao fazer json.loads em dados_json:", e)
            dados_json = {}

    dados_json["logradouro"] = {"id": id_logradouro}
    dados_json_str = json.dumps(dados_json, ensure_ascii=False)
    body = dados_json_str

    return body


def put_economico(economico, id_logradouro):
    id_economico = economico["id_economico"]
    if not id_economico:
        log.error("PUT -> id_economico não encontrado.")
        return
    
    body = definir_body_put(economico, id_logradouro)
    if not body:
        log.info("Nenhum body 'definir_body_put()'.")
        return
    
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/economicos/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.put(url_ + str(id_economico), data=body ,headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        economico["situacao"] = f"ERRO -> {response.status_code} {response.text}"
        inserir_economico_dados(economico)
        return

    economico["situacao"] = "SUCESSO"
    inserir_economico_dados(economico)
    retorno = response.json()
    
    id_economico_ret = retorno["id"]
    logradouro_ret = retorno["logradouro"]

    ret_economico = {"id_economico": id_economico_ret, "logradouro": logradouro_ret} 
    
    log.info(f"-> PUT Response text: {ret_economico}")

    return ret_economico

