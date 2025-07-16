import json, requests, os
from tqdm import tqdm
from src import _logger as log, loader, db_manager
from dotenv import load_dotenv


load_dotenv()


def job(ids_logradouros_filtro, id_logradouro_novo):
    log.info("Iniciando o job de atualização de Plantas de Valores -> Logradouros.")
        
    if not ids_logradouros_filtro:
        log.error("ids_logradouros_filtro vazio.")
        return
    if not id_logradouro_novo:
        log.error("id_logradouro_novo vazio.")
        return
    
    for logradouro in ids_logradouros_filtro:

        planta_valor = get_plantas_filtro(logradouro)
        if not planta_valor:
            log.error(f"Sem registros de planta_valor para o ID_LOGRADOURO: {logradouro}.")
            continue

        for planta in tqdm(planta_valor, desc=f"Atualizando Plantas [Logradouro -> {logradouro}]"):
            id_planta = planta["id_planta"]
            getrow = getrows_planta(id_planta)
            if getrow:
                log.info(f"ID_PLANTA: {id_planta}, já processado.")
                continue

            log.info(f"Processando ID_PLANTA ->{id_planta}")
            planta_dados = get_planta(id_planta)
            
            put_planta(planta_dados, id_logradouro_novo)


def get_plantas_filtro(id_logradouro):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/core/plantasValores"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    limit = 100
    offset = 0
    todos_plantas = []
    has_next = True

    while has_next:
        params_ = {
            "filter": f"(idLogradouros in ({id_logradouro}))",
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
        ids_plantas = [{"id_planta": item["id"], "id_logradouro": id_logradouro} for item in itens if "id" in item]
        todos_plantas.extend(ids_plantas)

        has_next = retorno.get("hasNext", False)
        offset += limit

    log.info(f"-> Total de Plantas de Valores filtrados: {len(todos_plantas)}")
    
    return todos_plantas


def get_planta(id_planta):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/core/plantasValores/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.get(url_ + str(id_planta), headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        return

    retorno = response.json()
    
    id_planta_ret = retorno["id"]
    codigo_planta_ret = f"{retorno["ano"]}/{retorno["zoneamento"]}"
    id_logradouros_ret = retorno["logradouros"].get("id")
    dados_json = json.dumps(retorno, ensure_ascii=False)
    # dados_json = retorno

    ret_planta = {"id_planta": id_planta_ret, 
                  "codigo_planta": codigo_planta_ret,
                  "id_logradouro": id_logradouros_ret, 
                  "dados_json": dados_json} 
    
    return ret_planta

def inserir_planta_dados(planta):
    if not planta:
        log.info("Nenhuma Planta de Valor para inserir.")
        return

    tabela_nome = "planta_valores_logradouro_dados"
    colunas = [
        'id_planta',
        'codigo_planta',
        'situacao',
        'dados_json',
    ]
    sql_statements = loader.generate_insert_data_sql(tabela_nome, colunas, [planta])
    
    if not sql_statements:
        log.info("Nenhum SQL gerado para inserção.")
        return

    loader.execute_sql_statements(sql_statements)
    log.info(f"Planta de Valor inserido: {planta["id_planta"]}")  


def getrows_planta(id_planta):
    try:
        with db_manager.connect() as cnx:
            with cnx.cursor() as c:
                c.execute(
                    f"""
                    select
                        id_planta,
                        codigo_planta,
                        situacao
                    from planta_valores_logradouro_dados
                    where id_planta = '{id_planta}'
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None


def definir_body_put(planta, id_logradouro):

    if not planta:
        log.info("Nenhum resultado de 'planta_valores_dados'.")
        return
    
    if not id_logradouro:
        log.info("Nenhum id_logradouro foi informado.")
        return

    dados_json = planta["dados_json"]
    if isinstance(dados_json, str):
        try:
            dados_json = json.loads(dados_json)
        except json.JSONDecodeError as e:
            print("Erro ao fazer json.loads em dados_json:", e)
            dados_json = {}

    dados_json["logradouros"] = {"id": id_logradouro}
    dados_json_str = json.dumps(dados_json, ensure_ascii=False)
    body = dados_json_str

    return body


def put_planta(planta, id_logradouro):
    id_planta = planta["id_planta"]
    if not id_planta:
        log.error("PUT -> id_planta não encontrado.")
        return
    
    body = definir_body_put(planta, id_logradouro)
    if not body:
        log.info("Nenhum body 'definir_body_put()'.")
        return
    
    url_ = "https://tributos.betha.cloud/tributos/v1/api/core/plantasValores/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.put(url_ + str(id_planta), data=body ,headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        planta["situacao"] = f"ERRO -> {response.status_code} {response.text}"
        inserir_planta_dados(planta)
        return

    planta["situacao"] = "SUCESSO"
    inserir_planta_dados(planta)
    retorno = response.json()
    
    id_planta_ret = retorno["id"]
    logradouro_ret = retorno["logradouros"]

    ret_planta = {"id_planta": id_planta_ret, "logradouro": logradouro_ret} 
    
    log.info(f"-> PUT Response text: {ret_planta}")

    return ret_planta

