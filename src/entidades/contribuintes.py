import json, requests, os
from tqdm import tqdm
from src import _logger as log, loader, db_manager
from dotenv import load_dotenv


load_dotenv()


def job(ids_bairros_filtro, id_bairro_novo):
    log.info("Iniciando o job de atualização de Contribuintes -> Bairros.")
        
    if not ids_bairros_filtro:
        log.error("ids_bairros_filtro vazio.")
        return
    if not id_bairro_novo:
        log.error("id_bairro_novo vazio.")
        return
    
    for bairro in ids_bairros_filtro:

        contribuintes = get_contribuintes_filtro(bairro)
        if not contribuintes:
            log.error(f"Sem registros de contribuintes para o ID_BAIRRO: {bairro}.")
            continue

        for contribuinte in tqdm(contribuintes, desc=f"Atualizando Contribuinte [Bairro -> {bairro}]"):
            id_contribuinte = contribuinte["id_contribuinte"]
            getrow = getrows_contribuinte(id_contribuinte)
            if getrow:
                log.info(f"ID_CONTRIBUINTE: {id_contribuinte}, já processado.")
                continue

            log.info(f"Processando ID_CONTRIBUINTE ->{id_contribuinte}")
            contribuinte_dados = get_contribuinte(id_contribuinte)
            if not contribuinte_dados:
                log.error(f"Falha ao consultar(GET) ID_CONTRIBUINTE: {id_contribuinte}")
                continue

            put_contribuinte(contribuinte_dados, id_bairro_novo)


def get_contribuintes_filtro(id_bairro):
    getrows = getrows_contribuintes_filtro(id_bairro)
    if not getrows:
        log.info(f"Sem contribuintes para o ID_BAIRRO: {id_bairro}.")
        return
    todos_contribuintes = []
    
    for item in getrows:
        id_contribuinte = item[0]
        id_bairro = item[1]

        ids_contribuinte = [{"id_contribuinte": id_contribuinte, "id_bairro": id_bairro}]
        todos_contribuintes.extend(ids_contribuinte)
        
    log.info(f"-> Total de Constribuintes filtrados: {len(todos_contribuintes)}")
    
    return todos_contribuintes


def get_contribuinte(id_contribuinte):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/contribuintes/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.get(url_ + str(id_contribuinte), headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        return

    retorno = response.json()
    
    id_contribuinte_ret = retorno["id"]
    codigo_contribuinte_ret = retorno["codigo"]
    id_bairro_ret = retorno["enderecos"][0]["bairro"].get("id")
    dados_json = json.dumps(retorno, ensure_ascii=False)
    # dados_json = retorno

    ret_contribuinte = {"id_contribuinte": id_contribuinte_ret, 
                  "codigo_contribuinte": codigo_contribuinte_ret,
                  "id_bairro": id_bairro_ret, 
                  "dados_json": dados_json} 
    
    return ret_contribuinte

def inserir_contribuinte_dados(contribuinte):
    if not contribuinte:
        log.info("Nenhum Contribuinte para inserir.")
        return

    tabela_nome = "contribuintes_dados"
    colunas = [
        'id_contribuinte',
        'codigo_contribuinte',
        'situacao',
        'dados_json',
    ]
    sql_statements = loader.generate_insert_data_sql(tabela_nome, colunas, [contribuinte])
    
    if not sql_statements:
        log.info("Nenhum SQL gerado para inserção.")
        return

    loader.execute_sql_statements(sql_statements)
    log.info(f"Contribuinte inserido: {contribuinte["id_contribuinte"]}")  


def getrows_contribuinte(id_contribuinte):
    try:
        with db_manager.connect() as cnx:
            with cnx.cursor() as c:
                c.execute(
                    f"""
                    select
                        id_contribuinte,
                        codigo_contribuinte,
                        situacao
                    from contribuintes_dados
                    where id_contribuinte = '{id_contribuinte}' 
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None
    

def getrows_contribuintes_filtro(id_bairro):
    try:
        with db_manager.connect() as cnx:
            with cnx.cursor() as c:
                c.execute(
                    f"""
                    select
                        id_contribuinte,
                        id_bairro
                    from contribuintes_filtro
                    where id_bairro = '{id_bairro}'
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None


def definir_body_put(contribuinte, id_bairro):

    if not contribuinte:
        log.info("Nenhum resultado de 'contribuintes_dados' .")
        return
    
    dados_json = contribuinte["dados_json"]
    if isinstance(dados_json, str):
        try:
            dados_json = json.loads(dados_json)
        except json.JSONDecodeError as e:
            print("Erro ao fazer json.loads em dados_json:", e)
            dados_json = {}
    
    dados_json["enderecos"][0]["bairro"] = {"id": id_bairro}
    dados_json_str = json.dumps(dados_json, ensure_ascii=False)
    body = dados_json_str

    return body


def put_contribuinte(contribuinte, id_bairro):
    id_contribuinte = contribuinte["id_contribuinte"]
    if not id_contribuinte:
        log.error("PUT -> id_contribuinte não encontrado.")
        return
    
    body = definir_body_put(contribuinte, id_bairro)

    if not body:
        log.info("Nenhum body 'definir_body_put()'.")
        return
    
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/referentes/contribuintes/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.put(url_ + str(id_contribuinte), data=body ,headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        contribuinte["situacao"] = f"ERRO -> {response.status_code} {response.text}"
        inserir_contribuinte_dados(contribuinte)
        return

    contribuinte["situacao"] = "SUCESSO"
    inserir_contribuinte_dados(contribuinte)
    retorno = response.json()
    
    id_contribuinte_ret = retorno["id"]
    bairro_ret = retorno["enderecos"][0]["bairro"]

    ret_contribuinte = {"id_contribuinte": id_contribuinte_ret, "bairro": bairro_ret} 
    
    log.info(f"-> PUT Response text: {ret_contribuinte}")

    return ret_contribuinte

