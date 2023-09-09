import openai
import pandas as pd
import random
import json

# Configurar a chave da API
api_key = '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'
openai.api_key = api_key

# Função principal para executar o pipeline ETL


def main():
    dados = extrair_dados()
    dados_transformados = [transformar_dados(dado) for dado in dados]
    salvar_dados(dados_transformados)

# Função para extrair dados


def extrair_dados():
    df = pd.read_csv("usuarios.csv")
    users = df['User'].tolist()
    return users

# Função para transformar dados


def transformar_dados(nome):
    sobrenomes = pegar_sobrenomes()
    sobrenome_aleatorio = random.choice(sobrenomes)
    return {"nome": nome, "sobrenome": sobrenome_aleatorio}

# Função para pegar sobrenomes


def pegar_sobrenomes():
    carregar_sobrenomes()
    df = pd.read_csv("sobrenomes.csv")
    sobrenomes = df['Sobrenome'].tolist()
    return sobrenomes

# Função para carregar sobrenomes


def carregar_sobrenomes():
    sobrenomes = ["Silva", "Santos", "Oliveira", "Pereira", "Almeida", "Ferreira", "Rodrigues", "Souza", "Lima", "Costa", "Araújo", "Carvalho", "Gonçalves", "Ribeiro", "Martins", "Mendes", "Barbosa", "Nascimento", "Machado", "Freitas", "Azevedo", "Moreira",
                  "Dias", "Fernandes", "Sales", "Gomes", "Cardoso", "Ramos", "Teixeira", "Castro", "Monteiro", "Coelho", "Sousa", "Cavalcanti", "Leal", "Correia", "Cruz", "Dantas", "Campos", "Lopes", "Nunes", "Morais", "Lima", "Correia", "Pinto", "Borges", "Vieira"]

    df = pd.DataFrame({"Sobrenome": sobrenomes})
    df.to_csv("sobrenomes.csv", index=False)

# Função para salvar os dados transformados em CSV


def salvar_dados(dados):
    df = pd.DataFrame(dados)
    df.to_csv("dados-alterados.csv", index=False)


if __name__ == "__main__":
    main()
