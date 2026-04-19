# Guia de Extensao

## Como adicionar uma nova origem

1. Criar um adapter em `internal/adapters/source/<nome>`
2. Implementar a interface `core.Source`
3. Registrar o adapter em `internal/core/factory.go`
4. Adicionar configuracao correspondente em `internal/config`
5. Criar um ADR se a nova origem introduzir semantica nova de checkpoint

## Como adicionar um novo destino

1. Criar um adapter em `internal/adapters/sink/<nome>`
2. Implementar a interface `core.Sink`
3. Registrar o adapter em `internal/core/factory.go`
4. Adicionar configuracao correspondente em `internal/config`
5. Criar um ADR se o destino exigir semantica nova de idempotencia ou naming

## Regras de design

- o `Runner` nao pode depender de SDKs concretos
- adapters nao devem conhecer detalhes de outros adapters
- o modelo interno deve continuar pequeno e estavel
- nao generalizar contratos antes de existir necessidade real

## Criterios para novos contratos

Criar uma nova porta apenas quando:

- houver ao menos duas implementacoes concretas com comportamento comum
- a abstracao reduzir duplicacao real
- a interface ficar pequena e semanticamente clara
