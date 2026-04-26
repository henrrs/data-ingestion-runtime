# Arquitetura

Este diretorio concentra a documentacao arquitetural da solucao.

## Objetivo

Registrar decisoes que afetam:

- extensibilidade para novas origens e destinos
- semantica de entrega e resiliencia
- formato da landing zone
- padroes de modularizacao e ownership

## Principio Atual Da Landing Zone

A Landing Zone deste repositorio deve permanecer:

- raw e append-only
- imutavel e fiel ao Kafka
- barata de escrever
- livre de deduplicacao e parsing analitico pesado

A etapa de Bronze/Delta vem depois e nao deve ser empurrada para o runtime Go.

## Estrategia de documentacao

Usaremos tres niveis de documentacao:

1. `ADR` para decisoes arquiteturais irreversiveis ou caras de mudar
2. `Visao do sistema` para explicar os componentes e o fluxo atual
3. `Guias operacionais e de extensao` para orientar evolucao futura

## Indice

- [system-overview.md](system-overview.md)
- [current-connector-flow.md](current-connector-flow.md)
- [performance-analysis.md](performance-analysis.md)
- [implementation-plan-performance.md](implementation-plan-performance.md)
- [decisions/0001-hexagonal-architecture.md](decisions/0001-hexagonal-architecture.md)
- [decisions/0002-landing-avro-raw-payload.md](decisions/0002-landing-avro-raw-payload.md)
- [decisions/0003-batch-scale-to-zero.md](decisions/0003-batch-scale-to-zero.md)
- [extension-guide.md](extension-guide.md)

## Regra de atualizacao

Sempre criar ou atualizar um ADR quando houver mudanca em:

- padrao arquitetural do core
- contrato de `Source`, `Sink` ou `Serializer`
- semantica de commit/checkpoint
- formato e schema da landing zone
- estrategia oficial de testes E2E
