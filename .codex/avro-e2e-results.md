# Avro E2E Benchmark Results

## Contexto

Benchmark E2E executado com:

- formato: `avro`
- compressao: `snappy`
- eventos: `100k`, `300k`, `500k`
- payload: `8192` bytes pseudoaleatorio
- particoes: `6`
- `include_headers=true`
- `include_key=true`
- limpeza de Redpanda e MinIO antes de cada cenario

Arquivos de origem:

- `.codex/avro-e2e-matrix-fixed-20260425T235714Z/summary.csv`
- `.codex/avro-e2e-matrix-300k-rest-20260426T003107Z/summary.csv`
- `.codex/avro-e2e-matrix-500k-20260426T005549Z/summary.csv`

## Resultado 100k

| flush:encode:upload | tempo conector | throughput | RSS KB | lag | objetos | tamanho bytes |
|---|---:|---:|---:|---:|---:|---:|
| `4:4:4` | `57.34s` | `1743.98 rec/s` | `604664` | `0` | `12` | `832581969` |
| `1:1:1` | `72.84s` | `1372.87 rec/s` | `622176` | `0` | `12` | `832567896` |
| `2:2:4` | `76.71s` | `1303.61 rec/s` | `637192` | `0` | `12` | `832552064` |
| `2:2:2` | `77.60s` | `1288.66 rec/s` | `595508` | `0` | `12` | `832547439` |
| `2:4:2` | `85.87s` | `1164.55 rec/s` | `626212` | `0` | `12` | `832564526` |
| `4:2:2` | `91.14s` | `1097.21 rec/s` | `609260` | `0` | `12` | `832562637` |

Melhor cenario: `4:4:4`.

## Resultado 300k

| flush:encode:upload | tempo conector | throughput | RSS KB | lag | objetos | tamanho bytes |
|---|---:|---:|---:|---:|---:|---:|
| `2:2:2` | `185.23s` | `1619.61 rec/s` | `595564` | `0` | `33` | `2497787204` |
| `1:1:1` | `187.38s` | `1601.02 rec/s` | `584148` | `0` | `33` | `2497771983` |
| `4:4:4` | `217.00s` | `1382.49 rec/s` | `603196` | `0` | `33` | `2497745804` |
| `2:2:4` | `232.48s` | `1290.43 rec/s` | `593388` | `0` | `33` | `2497771877` |
| `2:4:2` | `259.00s` | `1158.30 rec/s` | `594916` | `0` | `33` | `2497788598` |
| `4:2:2` | `318.57s` | `941.71 rec/s` | `608124` | `0` | `33` | `2497795335` |

Melhor cenario: `2:2:2`.

## Resultado 500k

| flush:encode:upload | tempo conector | throughput | RSS KB | lag | objetos | tamanho bytes |
|---|---:|---:|---:|---:|---:|---:|
| `2:2:2` | `297.66s` | `1679.77 rec/s` | `597444` | `0` | `54` | `4163095936` |
| `1:1:1` | `310.56s` | `1609.99 rec/s` | `619100` | `0` | `54` | `4163080070` |
| `4:4:4` | `337.46s` | `1481.66 rec/s` | `607600` | `0` | `54` | `4163089774` |
| `4:2:2` | `385.31s` | `1297.66 rec/s` | `589744` | `0` | `54` | `4163097830` |
| `2:4:2` | `392.95s` | `1272.43 rec/s` | `596588` | `0` | `54` | `4163072340` |
| `2:2:4` | `407.40s` | `1227.30 rec/s` | `614728` | `0` | `54` | `4163092820` |

Melhor cenario: `2:2:2`.

## Leitura

- `2:2:2` e o melhor ponto operacional para cargas maiores neste ambiente.
- `4:4:4` vence no `100k`, mas perde no `300k` e `500k`.
- Aumentar apenas uma dimensao de concorrencia piorou o resultado, especialmente `4:2:2`, `2:4:2` e `2:2:4`.
- RSS ficou relativamente estavel, em torno de `584 MB` a `637 MB`, mesmo nos cenarios maiores.
- Todos os cenarios finais fecharam com `lag=0`.

## Validacao

- `go test ./...`: passou
- `go vet ./...`: passou
- `go test ./... -bench=. -benchmem`: passou

Benchmarks Go relevantes:

| benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkStreamWriter` Avro | `2965` | `5037` | `1` |
| `BenchmarkNewKafkaMessageHeadersOff` | `17.58` | `0` | `0` |
| `BenchmarkNewKafkaMessageHeadersOn` | `635.9` | `112` | `2` |

## Recomendacao

Usar como default operacional local:

```yaml
runtime:
  max_parallel_flushes: 2
  max_parallel_encodes: 2
  max_parallel_uploads: 2
```

O proximo gargalo mais promissor parece estar menos em "mais goroutines" e mais em reduzir custo de temp file/readback e melhorar o caminho de upload, mantendo a semantica de commit somente apos persistencia.
