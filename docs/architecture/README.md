# Arquitetura

Este diretorio concentra a documentacao arquitetural da solucao.

## Objetivo

Registrar decisoes que afetam:

- extensibilidade para novas origens e destinos
- semantica de entrega e resiliencia
- formato da landing zone
- padroes de modularizacao e ownership

## Estrategia de documentacao

Usaremos tres niveis de documentacao:

1. `ADR` para decisoes arquiteturais irreversiveis ou caras de mudar
2. `Visao do sistema` para explicar os componentes e o fluxo atual
3. `Guias operacionais e de extensao` para orientar evolucao futura

## Indice

- [system-overview.md](C:\Users\henrr\Documents\Codex\2026-04-19-o-que-eu-posso-fazer-com\docs\architecture\system-overview.md)
- [decisions/0001-hexagonal-architecture.md](C:\Users\henrr\Documents\Codex\2026-04-19-o-que-eu-posso-fazer-com\docs\architecture\decisions\0001-hexagonal-architecture.md)
- [decisions/0002-landing-parquet-raw-payload.md](C:\Users\henrr\Documents\Codex\2026-04-19-o-que-eu-posso-fazer-com\docs\architecture\decisions\0002-landing-parquet-raw-payload.md)
- [decisions/0003-batch-scale-to-zero.md](C:\Users\henrr\Documents\Codex\2026-04-19-o-que-eu-posso-fazer-com\docs\architecture\decisions\0003-batch-scale-to-zero.md)
- [extension-guide.md](C:\Users\henrr\Documents\Codex\2026-04-19-o-que-eu-posso-fazer-com\docs\architecture\extension-guide.md)

## Regra de atualizacao

Sempre criar ou atualizar um ADR quando houver mudanca em:

- padrao arquitetural do core
- contrato de `Source`, `Sink` ou `Serializer`
- semantica de commit/checkpoint
- formato e schema da landing zone
- estrategia oficial de testes E2E
