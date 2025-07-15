from src import _logger as log, utils
from src.entidades import imoveis, economicos, planta_valores, contribuintes
from datetime import datetime


def main():

    # 5183919 - "UNIFICA1"
    # 5183939 - "UNIFICA2"
    ID_BAIRRO_FILTRO = [5183939]
    ID_BAIRRO_NOVO = 5183919

    # IMOVEIS
    print('')
    print(" # Iniciando o processo de Unificação de IMÓVEIS -> Bairros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")
    time_inicio = datetime.now()

    if resposta:
        imoveis.job(ID_BAIRRO_FILTRO, ID_BAIRRO_NOVO)

    # ECONOMICOS
    print('')
    print(" # Iniciando o processo de Unificação de ECONÔMICOS -> Bairros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")

    if resposta:
        economicos.job(ID_BAIRRO_FILTRO, ID_BAIRRO_NOVO)

    # PLANTA VALORES
    print('')
    print(" # Iniciando o processo de Unificação de PLANTA DE VALORES -> Bairros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")

    if resposta:
        planta_valores.job(ID_BAIRRO_FILTRO, ID_BAIRRO_NOVO)

    # CONTRIBUINTES
    print('')
    print(" # Iniciando o processo de Unificação de CONTRIBUINTES -> Bairros. # ")
    resposta = input("Executar? (s/N): ").strip().lower() == "s"
    print("Executando." if resposta else "Execução cancelada. ")

    if resposta:
        contribuintes.job(ID_BAIRRO_FILTRO, ID_BAIRRO_NOVO)        

    time_fim = datetime.now()
    print('')
    print(f"Processo iniciado em: {time_inicio}")
    print(f"Finalizado em: {time_fim}")
    print(f"Duração total: {time_fim - time_inicio}")
    
        
if __name__ == "__main__":
    main()

