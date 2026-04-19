# ADR 0001: Hexagonal Architecture com adapters de Source e Sink

## Status

Aceita

## Contexto

A solucao precisa crescer para novas origens e novos destinos sem duplicar o core de batching, commit, observabilidade e serializacao.

## Decisao

Adotar arquitetura hexagonal com:

- `Source` como porta de entrada
- `Sink` como porta de saida
- `Runner` como application service
- adapters concretos isolados por tecnologia

## Consequencias

### Positivas

- facilita testes
- reduz acoplamento tecnologico
- permite evolucao incremental

### Negativas

- adiciona mais modulos e interfaces
- exige disciplina para nao abstrair demais cedo demais
