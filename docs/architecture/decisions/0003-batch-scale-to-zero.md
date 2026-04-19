# ADR 0003: Ingestao batch com scale-to-zero

## Status

Aceita

## Contexto

O requisito principal e reduzir custo operacional de compute, sem necessidade de processamento em tempo real.

## Decisao

Adotar execucao batch orientada por scheduler externo, com:

- `Kubernetes CronJob`
- leitura periodica do Kafka
- commit manual apos escrita confirmada
- semantica `at-least-once`

## Consequencias

### Positivas

- menor custo computacional
- operacao previsivel
- melhor alinhamento com landing zone orientada a lote

### Negativas

- maior risco de backlog se a janela for mal dimensionada
- exigencia maior de observabilidade e replay
