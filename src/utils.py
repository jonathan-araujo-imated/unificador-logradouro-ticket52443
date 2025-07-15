import os
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm


load_dotenv()


def sql_log_erro(erro):
    if erro:
        path = os.getenv("PATH_SQL_LOG_ERRO")
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(path, "a", encoding="utf-8") as f:
            for item in erro:
                f.write(f"{timestamp}: {item}")
        print(f"Erro registrado em: {path}")


def ddl_insert_log(statements):
    path = os.getenv("PATH_SQL_LOG_INSERT")
    with open(path, "a", encoding="utf-8") as f:
        for comando in tqdm(statements, desc="Salvando LOG INSERTs"):
            f.write(' '.join(comando) + "\n")
    print(f"Comandos SQL Insert Into salvos em: {path}")

