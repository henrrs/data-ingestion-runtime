# ADR 0002: Landing zone em Parquet com payload bruto

## Status

Aceita

## Contexto

A landing zone deve preservar o evento de origem sem acoplar a ingestao a um schema rigidamente materializado. Ao mesmo tempo, a camada precisa ser eficiente para armazenamento e leitura posterior.

## Decisao

Usar:

- Parquet como formato fisico
- compressao ZSTD
- payload bruto preservado em `payload_raw`
- colunas tecnicas do Kafka para rastreabilidade

## Consequencias

### Positivas

- maior resiliencia a schema drift
- boa eficiencia de storage
- transicao natural para Bronze/Delta

### Negativas

- a landing nao e a camada ideal para analytics de negocio
- materializacao semantica fica deslocada para a Bronze
