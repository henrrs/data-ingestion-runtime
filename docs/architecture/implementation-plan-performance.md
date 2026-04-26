# Plano de Implementacao de Performance

Este documento resume a direcao atual do runtime, agora convergido para um unico write path `avro`, com foco em landing raw append-only.

## Objetivos

As frentes prioritarias sao:

1. maximizar throughput do write path Avro
2. reduzir RSS, heap e pressao de GC
3. manter commit somente apos persistencia confirmada
4. medir concorrencia, memoria e page cache de forma repetivel
5. preparar a evolucao futura para upload streaming sem quebrar a semantica de path por faixa de offsets

## Estado Atual

- `avro` e o formato operacional padrao
- `parquet` foi removido do runtime
- o assembler escreve incrementalmente e nao materializa `[]LandingRecord` completo no `BatchWindow`
- o flush e controlado por `max_parallel_flushes`
- encode local e upload continuam separados por limite de concorrencia
- `pprof` continua opcional para inspeções mais profundas

## Benchmark Operacional

Os scripts principais agora sao:

- [run-local-compression-matrix.sh](../../scripts/bench/run-local-compression-matrix.sh)
- [run-avro-e2e-matrix.sh](../../scripts/bench/run-avro-e2e-matrix.sh)

O benchmark E2E passa a medir:

- tempo total do produtor e do conector
- RSS via `/usr/bin/time`
- heap final, alloc delta, alloc rate e pausas de GC via logs do runtime
- page cache antes e depois de cada rodada
- lag final e volume persistido no MinIO

## Upload Streaming

Existe uma restricao arquitetural importante:

- o path final do objeto inclui `start_offset` e `end_offset`
- portanto o nome definitivo do arquivo so e conhecido quando a janela fecha

Isso significa que upload streaming direto para o objeto final exige uma destas estrategias:

1. escrever para um nome temporario remoto e renomear depois
2. mudar a semantica do path
3. voltar a materializar a janela antes do upload

Como o objetivo atual e preservar a semantica WAL-like e evitar regressao de memoria, essa evolucao deve ser tratada como uma fase separada.

## Proximos Passos

1. rodar a matriz Avro E2E com `100k`, `300k` e `500k`
2. comparar a matriz de `max_parallel_flushes`, `max_parallel_encodes` e `max_parallel_uploads`
3. identificar o melhor ponto operacional para `snappy`
4. avaliar se o proximo gargalo real esta em encode, upload ou page cache
