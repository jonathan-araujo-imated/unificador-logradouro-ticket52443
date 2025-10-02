from src import _logger as log, utils
from src.entidades import imoveis, economicos, planta_valores, contribuintes, secoes
from datetime import datetime


def main():

    # 10054571 - "codigo": 600,   "descricao": "Rua", "nome": "DAS ACACIAS", 
    # 10061415 - "codigo": 29342, "descricao": "Rua", "nome": "RUA ACACIAS",
    ID_LOGRADOURO_FILTRO = [10054571]
    ID_LOGRADOURO_NOVO = 10061415


    # IMOVEIS
    print('')
    print(" # Iniciando o processo de Unificação de IMÓVEIS -> Logradouros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")
    time_inicio = datetime.now()

    if resposta:
        imoveis.job(ID_LOGRADOURO_FILTRO, ID_LOGRADOURO_NOVO)

    # ECONOMICOS
    print('')
    print(" # Iniciando o processo de Unificação de ECONÔMICOS -> Logradouros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")

    if resposta:
        economicos.job(ID_LOGRADOURO_FILTRO, ID_LOGRADOURO_NOVO)

    # PLANTA VALORES
    print('')
    print(" # Iniciando o processo de Unificação de PLANTA DE VALORES -> Logradouros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")

    if resposta:
        planta_valores.job(ID_LOGRADOURO_FILTRO, ID_LOGRADOURO_NOVO)

    # CONTRIBUINTES
    print('')
    print(" # Iniciando o processo de Unificação de CONTRIBUINTES -> Logradouros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")

    if resposta:
        contribuintes.job(ID_LOGRADOURO_FILTRO, ID_LOGRADOURO_NOVO)    

    # SECOES
    print('')
    print(" # Iniciando o processo de Unificação de SEÇÔES -> Logradouros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")
    time_inicio = datetime.now()

    if resposta:
        secoes.job(ID_LOGRADOURO_FILTRO, ID_LOGRADOURO_NOVO)    


    time_fim = datetime.now()
    print('')
    print(f"Processo iniciado em: {time_inicio}")
    print(f"Finalizado em: {time_fim}")
    print(f"Duração total: {time_fim - time_inicio}")
    
        
if __name__ == "__main__":
    main()

