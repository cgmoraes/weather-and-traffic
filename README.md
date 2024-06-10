# Documentação do Repositório Weather and Traffic

## Visão Geral

Este repositório contém as configurações e scripts necessários para a execução de um sistema de processamento de dados sobre condições meteorológicas e tráfego utilizando Apache Airflow e Docker.

## Estrutura do Repositório

- **.gitignore**: Arquivos e diretórios ignorados pelo controle de versão.
- **dags**: Scripts Python para a criação de DAGs no Apache Airflow. Estes scripts definem as tarefas automatizadas que processam os dados de tráfego e condições meteorológicas.
- **docker-compose.yaml**: Arquivo para configuração do ambiente com Docker Compose, facilitando a implantação dos serviços necessários.
- **sql**: Contém scripts SQL para a criação e manipulação de banco de dados relacionados aos dados processados.
- **view**: Scripts ou configurações utilizadas para a visualização dos dados processados.

## Configuração

### Pré-requisitos

- Docker
- Docker Compose
- Apache Airflow

### Instruções de Instalação

1. Clone o repositório para sua máquina local:
```
git clone [URL_DO_REPOSITÓRIO]
```

2. Navegue até o diretório do repositório:
```
cd weather-and-traffic
```

3. Execute o Docker Compose para iniciar os serviços:
```
docker-compose up -d
```


### Uso

Após a inicialização dos serviços via Docker Compose, acesse o Apache Airflow através do navegador para visualizar e gerenciar as DAGs.

## Contribuições

Contribuições são bem-vindas! Por favor, crie uma issue ou envie um pull request com suas sugestões de melhorias ou correções.

## Licença

Especificar a licença sob a qual o projeto está disponibilizado.
