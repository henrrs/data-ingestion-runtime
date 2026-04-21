# Plano de Implementacao de Performance

Este documento transforma a analise de performance em um plano executavel no codigo, com foco em throughput, controle de memoria e previsibilidade operacional.

## Objetivos

As frentes de trabalho sao:

1. medir o custo real das opcoes de compressao
2. estabilizar a etapa de upload com tamanho conhecido
3. desacoplar consumo e flush com concorrencia controlada
4. adicionar instrumentacao para diagnostico com `pprof`
5. decidir com dados se ainda vale otimizar mais a camada Parquet ou abrir uma POC com Arrow

## Status Atual

- `benchmark A/B`: implementado via script local para matriz de compressao
- `upload com tamanho conhecido`: implementado em MinIO e ADLS com arquivo temporario local
- `flush assincrono controlado`: implementado no `Runner` por particao com limite global de concorrencia
- `pprof opcional`: implementado e desabilitado por padrao
- `avaliacao de stack Parquet/Arrow`: pendente de novos perfis e benchmarks

## Fase 1. Benchmark A/B de compressao

Objetivo:

- comparar `zstd`, `snappy` e `uncompressed` no mesmo workload

Entrega no repositorio:

- [scripts/bench/run-local-compression-matrix.sh](../../scripts/bench/run-local-compression-matrix.sh)

Saidas esperadas:

- tempos do produtor e do conector
- pico de memoria reportado por `/usr/bin/time`
- logs separados por compressao

## Fase 2. Upload com tamanho conhecido

Objetivo:

- eliminar a penalidade de upload com tamanho `-1`

Mudanca aplicada:

- o Parquet e materializado em arquivo temporario local
- o upload acontece usando o tamanho real do arquivo
- o arquivo temporario e removido ao final

Arquivos:

- [internal/adapters/sink/parquetutil/writer.go](../../internal/adapters/sink/parquetutil/writer.go)
- [internal/adapters/sink/minio/sink.go](../../internal/adapters/sink/minio/sink.go)
- [internal/adapters/sink/adls/sink.go](../../internal/adapters/sink/adls/sink.go)

## Fase 3. Flush assincrono por particao

Objetivo:

- permitir sobreposicao entre leitura e escrita sem perder ordem por particao

Mudanca aplicada:

- o `Runner` roteia mensagens por particao
- cada particao possui um worker proprio de batch
- o flush e limitado globalmente por `runtime.max_parallel_flushes`
- o enfileiramento por particao e limitado por `runtime.partition_queue_size`
- o commit continua acontecendo somente depois de escrita confirmada

Arquivo principal:

- [internal/app/runner.go](../../internal/app/runner.go)

## Fase 4. Instrumentacao com `pprof`

Objetivo:

- medir CPU, heap, allocs e goroutines durante benchmark

Configuracao:

```yaml
runtime:
  pprof_enabled: true
  pprof_addr: 127.0.0.1:6060
```

Arquivo principal:

- [cmd/landing-connector/main.go](../../cmd/landing-connector/main.go)

## Fase 5. Decisao sobre otimizacoes profundas

Esta fase continua aberta e deve ser guiada pelos novos perfis:

- se o gargalo continuar em serializacao, atacar `parquetutil`
- se o gargalo continuar em layout de memoria, avaliar schema e representacao
- so considerar Arrow se houver evidencias de que o custo de conversao para modelo colunar compensa a complexidade adicional

## Configuracao Nova

O conector agora aceita:

```yaml
runtime:
  max_parallel_flushes: 4
  partition_queue_size: 8
  pprof_enabled: false
  pprof_addr: 127.0.0.1:6060
```

Observacoes:

- `max_parallel_flushes` controla quantos flushes podem gravar em paralelo
- `partition_queue_size` define o tamanho do buffer por particao antes de aplicar backpressure
- `pprof` fica desabilitado por padrao

## Proxima rodada recomendada

1. rodar a matriz de compressao com o novo `Runner`
2. comparar `zstd`, `snappy` e `uncompressed`
3. coletar perfis `pprof` durante o melhor e o pior caso
4. decidir o proximo experimento na camada Parquet
