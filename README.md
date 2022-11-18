# Black Belt Analytics 2022
![AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)
![BlackBelt](https://img.shields.io/badge/BlackBelt-Analytics%202022-orange?style=for-the-badge)
![GFT](https://img.shields.io/badge/-GFT%20GROUP-blue?style=for-the-badge)

![diagrama](https://github.com/jvjfranca/bigdatapipelne/blob/main/img/diagrama.png)

> Este projeto consiste em analise de dados em tempo real e historica utilizando AWS Kinesis e AWS Glue.
> Utilizado o DataOps Development Kit e Cloud Development Kit em sua construcao.


| Diretorio/Arquivo          | Descricao                                  | Observacoes                                                                                                |
| -------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| ddk_app                    | zyx                                        | zyx                                                                                                        |
| ddk_app/custom             | Modulo com construtores DDK e CDK          | Construtores personalizados DDK / CDK utilizados nas stacks                                                |
| ddk_app/ddk_app_stack.py   | Stack DDK/CDK                              | Instanciamento de Stacks Analytics e Criptografia                                                          |
| ddk_app/generator_stack.py | Stack gerador de transacoes de cartao      | Instanciamento de Stack servico gerador de transacoes em Python                                            |
| flink_app                  | Aplicacao Kinesis Analytics                | Analisa e filtra transcoes superiores a 5k enviando para um Kinesis Data Stream                            |
| generator_app              | Aplicacao geradora de transacoes de cartao | Utilizado um servico Fargate com consultas API nominatim para geracao de geolocalizacao                    |
| glue_scripts               | PySpark Scripts                            | Jobs utilizados pelo Glue                                                                                  |
| lambda_app                 | Scripts Python                             | Aplicacoes utilizadas para Backend API Gateway e consumidor Kinesis Data Stream (fluxo Realtime Analytics) |
| app.py                     | Script Principal DDK-CDK                   | Ponto de entrada do projeto, definicoes de pipeline CI/CD                                                  |
| cdk.json                   | Configuracao CDK                           |                                                                                                            |
| ddk.json                   | Configuracao DDK                           |                                                                                                            |
| requirements.txt           | Dependencias para execucao do projeto      |                                                                                                            |
b