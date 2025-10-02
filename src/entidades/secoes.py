import json, requests, os
from tqdm import tqdm
from src import _logger as log, loader, db_manager
from dotenv import load_dotenv


load_dotenv()


def job(ids_logradouros_filtro, id_logradouro_novo):
    log.info("Iniciando o job de atualização de Seções -> Logradouros.")
        
    if not ids_logradouros_filtro:
        log.error("ids_logradouros_filtro vazio.")
        return
    if not id_logradouro_novo:
        log.error("id_logradouro_novo vazio.")
        return
    
    todas_secoes = get_todas_secoes()
    if not todas_secoes:
        log.error("Sem registros de Seções.")
        return
    
    for secao in tqdm(todas_secoes, desc=f"Atualizando Seções"):
        
        if secao["id_logradouro"] in ids_logradouros_filtro:
            log.info(f"ID_SECAO: {secao['id_secao']}, relacionado com o logradouro ID: {secao["id_logradouro"]}.")

            print(secao)
            id_secao = secao["id_secao"]
            getrow = getrows_secao(id_secao)
            if getrow:
                log.info(f"ID_SECAO: {id_secao}, já processado.")
                continue

            log.info(f"Processando ID_SECAO ->{id_secao}")
            secao_dados = get_secao(id_secao)
            if not secao_dados:
                log.error(f"Falha ao consultar(GET) ID_SECAO: {id_secao}")
                continue

            # log.info(f"secao_dados ->{secao_dados}")
            put_contribuinte(secao_dados, id_logradouro_novo)



def get_todas_secoes():
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/enderecos/secoes"
           
    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    limit = 100
    offset = 0
    todas_secoes = []
    has_next = True
    
    while has_next:
        params_ = {
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
        secoes = [
            {
                "id_secao": item.get("id"),
                "id_logradouro": (item.get("logradouro") or {}).get("id"),
            }
            for item in itens
            if item.get("id") is not None and (item.get("logradouro") or {}).get("id") is not None
        ]
        todas_secoes.extend(secoes)

        has_next = retorno.get("hasNext", False)
        offset += limit

    log.info(f"-> Total de Secoes: {len(todas_secoes)}")
    
    return todas_secoes


def get_secoes_filtro(id_logradouro):
    url_ = "https://tributos.betha.cloud/tributos/dados/api/contribuintes/enderecos"
           
    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    limit = 100
    offset = 0
    todas_secoes_filtradas = []
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
        secoes_encontradas = [
            {
                "id_secao": item.get("id"),
                "id_logradouro": (item.get("logradouro") or {}).get("id"),
            }
            for item in itens
            if item.get("id") is not None and (item.get("logradouro") or {}).get("id") is not None
        ]
        todas_secoes_filtradas.extend(secoes_encontradas)

        has_next = retorno.get("hasNext", False)
        offset += limit

    log.info(f"-> Total de Secoes filtradas: {len(todas_secoes_filtradas)}")
    
    return todas_secoes_filtradas


def get_secao(id_secao):
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/enderecos/secoes/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.get(url_ + str(id_secao), headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        return

    retorno = response.json()
    
    id_secao_ret = retorno.get("id")
    logradouro_info = retorno.get("logradouro") or {}
    face_info = retorno.get("face") or {}
    face_descricao = face_info.get("descricao") or ""
    codigo_secao_ret = f"{retorno.get('nroSecao')}/{logradouro_info.get('codigo')}/{logradouro_info.get('nome')}"
    id_logradouro_ret = logradouro_info.get("id")
    dados_json = json.dumps(retorno, ensure_ascii=False)

    ret_secao = {"id_secao": id_secao_ret, 
                  "codigo_secao": codigo_secao_ret,
                  "id_logradouro": id_logradouro_ret, 
                  "dados_json": dados_json} 
    
    return ret_secao

def inserir_secao_dados(secao):
    if not secao:
        log.info("Nenhum Seção para inserir.")
        return

    tabela_nome = "secoes_logradouro_dados"
    colunas = [
        'id_secao',
        'codigo_secao',
        'situacao',
        'dados_json',
    ]
    sql_statements = loader.generate_insert_data_sql(tabela_nome, colunas, [secao])
    
    if not sql_statements:
        log.info("Nenhum SQL gerado para inserção.")
        return

    loader.execute_sql_statements(sql_statements)
    log.info(f"Seção inserido: {secao["id_secao"]}")  


def getrows_secao(id_secao):
    try:
        with db_manager.connect() as cnx:
            with cnx.cursor() as c:
                c.execute(
                    f"""
                    select
                        id_secao,
                        codigo_secao,
                        situacao
                    from secoes_logradouro_dados
                    where id_secao = '{id_secao}' 
                    """
                )

                return c.fetchall()
    except Exception as e:
        log.error("-> " + str(e))
        return None
    

def definir_body_put(secao, id_logradouro):

    if not secao:
        log.info("Nenhum resultado de 'secao_dados'.")
        return
    
    if not id_logradouro:
        log.info("Nenhum id_logradouro foi informado.")
        return

    dados_json = secao["dados_json"]
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


def put_contribuinte(secao, id_logradouro):
    id_secao = secao["id_secao"]
    if not secao:
        log.error("PUT -> id_secao não encontrado.")
        return
    
    body = definir_body_put(secao, id_logradouro)
    log.info(f"body -> {body}")
    
    if not body:
        log.info("Nenhum body 'definir_body_put()'.")
        return
    
    url_ = "https://tributos.betha.cloud/tributos/v1/api/cadastros/enderecos/secoes/"

    headers_ = {
        "Authorization": os.getenv("TOKEN"),
        "Content-Type": "application/json",
        "user-access": os.getenv("USER_ACESS")
    }

    try:
        response = requests.put(url_ + str(id_secao), data=body ,headers=headers_)
    except (Exception, requests.exceptions.RequestException) as e:
        log.error("-> " + str(e))
        return

    if response.status_code != 200:
        log.error(f"-> {response.status_code} {response.text}")
        secao["situacao"] = f"ERRO -> {response.status_code} {response.text}"
        inserir_secao_dados(secao)
        return

    secao["situacao"] = "SUCESSO"
    inserir_secao_dados(secao)
    retorno = response.json()
    
    id_secao_ret = retorno["id"]
    logradouro_ret = retorno["logradouro"]

    ret_secao = {"id_secao": id_secao_ret, "logradouroEnderecos": logradouro_ret} 
    
    log.info(f"-> PUT Response text: {ret_secao}")

    return ret_secao

