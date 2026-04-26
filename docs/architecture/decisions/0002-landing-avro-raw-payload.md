# ADR 0002: Landing zone em Avro com payload bruto

## Status

Aceita

## Contexto

A landing zone deste runtime deve priorizar:

- captura fiel do evento do Kafka
- custo baixo de escrita
- menor pressao de memoria e GC
- semantica append-only/WAL-like

O processamento analitico fica para a Bronze/Delta, fora do caminho quente do runtime Go.

## Decisao

Usar:

- Avro OCF como formato fisico
- `snappy` como compressao operacional padrao
- `payload_raw` preservado como `bytes`
- campos tecnicos do Kafka para rastreabilidade e replay

## Consequencias

### Positivas

- write path unico e mais simples de operar
- menor custo estrutural que o caminho colunar para captura raw
- melhor alinhamento com o objetivo de landing barata e fiel ao log de origem

### Negativas

- consultas analiticas na landing nao sao prioridade
- materializacao para Delta/Bronze continua sendo etapa posterior
- upload streaming direto para o objeto final continua limitado pela semantica de nome com faixa de offsets
