import json, requests, os
from tqdm import tqdm
from src import _logger as log, loader, db_manager
from dotenv import load_dotenv


load_dotenv()


def job(ids_logradouros_filtro, id_logradouro_novo):
    log.info("Iniciando o job de atualização de Contribuintes -> Logradouros.")
        
    if not ids_logradouros_filtro:
        log.error("ids_logradouros_filtro vazio.")
        return
    if not id_logradouro_novo:
        log.error("id_logradouro_novo vazio.")
        return
    
    for logradouro in ids_logradouros_filtro:

        contribuintes = get_contribuintes_filtro(logradouro)
        if not contribuintes:
            log.error(f"Sem registros de contribuintes para o ID_LOGRADOURO: {logradouro}.")
            continue

        for contribuinte in tqdm(contribuintes, desc=f"Atualizando Contribuinte [Logradouro -> {logradouro}]"):
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

            put_contribuinte(contribuinte_dados, id_logradouro_novo, logradouro)


def get_contribuintes_filtro(id_logradouro):
    url_ = "https://tributos.betha.cloud/tributos/dados/api/contribuintes/enderecos"
           
    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    limit = 100
    offset = 0
    todos_contribuintes = []
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
        ids_contribuinte = [{"id_contribuinte": item["idContribuinte"], "id_logradouro": id_logradouro} for item in itens if "idContribuinte" in item]
        todos_contribuintes.extend(ids_contribuinte)

        has_next = retorno.get("hasNext", False)
        offset += limit

    log.info(f"-> Total de Contribuintes filtrados: {len(todos_contribuintes)}")
    
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
    id_logradouro_ret = retorno["enderecos"][0]["logradouro"].get("id")
    listaEnderecos = retorno["enderecos"]
    dados_json = json.dumps(retorno, ensure_ascii=False)
    # dados_json = retorno

    ret_contribuinte = {"id_contribuinte": id_contribuinte_ret, 
                  "codigo_contribuinte": codigo_contribuinte_ret,
                  "lista_enderecos": listaEnderecos,
                  "id_logradouro": id_logradouro_ret, 
                  "dados_json": dados_json} 
    
    return ret_contribuinte

def inserir_contribuinte_dados(contribuinte):
    if not contribuinte:
        log.info("Nenhum Contribuinte para inserir.")
        return

    tabela_nome = "contribuintes_logradouro_dados"
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
                    from contribuintes_logradouro_dados
                    where id_contribuinte = '{id_contribuinte}' 
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None
    

def getrows_contribuintes_filtro(id_logradouro):
    try:
        with db_manager.connect() as cnx:
            with cnx.cursor() as c:
                c.execute(
                    f"""
                    select
                        id_contribuinte,
                        id_logradouro
                    from contribuintes_logradouro_filtro
                    where id_logradouro = '{id_logradouro}'
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None


def definir_body_put(contribuinte, id_logradouro, idLogadouroFiltro):

    if not contribuinte:
        log.info("Nenhum resultado de 'contribuintes_logradouro_dados'.")
        return
    
    if not id_logradouro:
        log.info("Nenhum id_logradouro informado.")
        return
    
    dados_json = contribuinte["dados_json"]
    if isinstance(dados_json, str):
        try:
            dados_json = json.loads(dados_json)
        except json.JSONDecodeError as e:
            print("Erro ao fazer json.loads em dados_json:", e)
            dados_json = {}

    enderecos = contribuinte["lista_enderecos"]
    for endereco in enderecos:
        #print("idEndereco",endereco["id"])
        if endereco["logradouro"].get("id") == idLogadouroFiltro:
            #print(endereco["bairro"].get("id"))
            endereco["logradouro"] = {"id":id_logradouro}
    
    dados_json["enderecos"] = enderecos
    # dados_json["enderecos"][0]["logradouro"] = {"id": id_logradouro}
    dados_json_str = json.dumps(dados_json, ensure_ascii=False)
    body = dados_json_str

    return body


def put_contribuinte(contribuinte, id_logradouro, idLogadouroFiltro):
    id_contribuinte = contribuinte["id_contribuinte"]
    if not id_contribuinte:
        log.error("PUT -> id_contribuinte não encontrado.")
        return
    
    body = definir_body_put(contribuinte, id_logradouro, idLogadouroFiltro)

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
    logradouro_ret = retorno["enderecos"]

    ret_contribuinte = {"id_contribuinte": id_contribuinte_ret, "logradouroEnderecos": logradouro_ret} 
    
    log.info(f"-> PUT Response text: {ret_contribuinte}")

    return ret_contribuinte

