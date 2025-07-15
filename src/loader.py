from src import utils, db_manager
from tqdm import tqdm


def generate_insert_data_sql(tabela_nome, colunas, registros):
    if registros is None or len(registros) == 0:
        return []

    sql_statements = []
    values_list = []
    LOTE = 500

    total_linhas = len(registros)

    for registro in registros:
        print(f"Criando INSERTs '{tabela_nome} Total {total_linhas}", end='\r')

        valores = []
        for coluna in colunas:
            valor = registro.get(coluna)

            if valor is None:
                valores.append('NULL')
            elif isinstance(valor, (int, float)):
                valores.append(str(valor))
            elif isinstance(valor, (str)):
                valores.append(f"'{valor.replace('\'', '\'\'')}'")  # Escapar aspas simples
            else:
                valores.append(f"'{str(valor).replace('\'', '\'\'')}'")

        values_str = '(' + ', '.join(valores) + ')'
        values_list.append(values_str)

        if len(values_list) == LOTE:
            sql = f'INSERT INTO {tabela_nome} ({", ".join(colunas)}) VALUES \n' + ',\n'.join(values_list) + ';'
            sql_statements.append(sql)
            values_list = []

    # Se sobrou algum registro fora do lote
    if values_list:
        sql = f'INSERT INTO {tabela_nome} ({", ".join(colunas)}) VALUES \n' + ',\n'.join(values_list) + ';'
        sql_statements.append(sql)

    return sql_statements


def execute_sql_statements(statements):
    erros_list = []
    try:
        conn = db_manager.connect()
        for statement in tqdm(statements, desc=f"Executando comandos SQL"):
            statements = ''.join(statement) + "\n" 
            try:
                db_manager.execute_query(conn, statements)
            except Exception as e:
                table_name = statements.split()[2] if len(statements.split()) > 2 else "###"
                erros_list.append(f"Tabela[{table_name}]: {str(e)}")
                erros_list.append(f"Commando: {statements}\n")

        db_manager.close_connection(conn)
    except Exception as e:
            print(f"ERRO na execução geral: {e}")
    
    utils.sql_log_erro(erros_list)
