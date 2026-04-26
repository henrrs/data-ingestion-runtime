# Performance Report

## Resumo

Este ciclo refatorou o caminho quente do runtime Kafka/Redpanda -> Landing Zone para privilegiar captura raw append-only, reduzir materializacao de lotes inteiros em memoria e aplicar backpressure real no flush. O objetivo foi deixar `avro` como fast path de landing e manter `parquet` como caminho compativel, mas nao como write path principal.

As principais mudancas foram:

- assembler agora escreve incrementalmente em arquivo temporario e deixa de carregar `[]LandingRecord` completo dentro do `BatchWindow`
- pipeline operacional desacoplado em `assemble -> temp encode -> upload -> commit`
- `max_parallel_flushes` passou a limitar o ciclo completo de flush ate commit/falha
- headers deixaram de ser materializados quando `include_headers=false`
- `time.Now().UTC()` saiu do hot path por mensagem e passou a ser capturado por poll batch
- coordinator de commit passou a coalescer janelas contiguas por particao
- limpeza de temp files em sucesso e falha foi reforcada
- benchmarks e testes cobrindo headers, flush concurrency, commit ordering e cleanup foram adicionados

## Arquivos alterados

- `README.md`
- `docs/architecture/README.md`
- `configs/orders.example.yaml`
- `configs/orders.minio.example.yaml`
- `scripts/bench/run-local-compression-matrix.sh`
- `internal/model/record.go`
- `internal/core/factory.go`
- `internal/config/config.go`
- `internal/config/config_test.go`
- `internal/adapters/source/kafka/source.go`
- `internal/adapters/source/kafka/source_test.go`
- `internal/adapters/source/kafka/source_bench_test.go`
- `internal/adapters/sink/avroutil/writer.go`
- `internal/adapters/sink/avroutil/writer_bench_test.go`
- `internal/adapters/sink/parquetutil/writer.go`
- `internal/adapters/sink/parquetutil/writer_bench_test.go`
- `internal/adapters/sink/fileformat/tempfile.go`
- `internal/adapters/sink/fileformat/tempfile_test.go`
- `internal/adapters/sink/minio/sink.go`
- `internal/adapters/sink/adls/sink.go`
- `internal/batch/assembler.go`
- `internal/batch/assembler_test.go`
- `internal/app/runner.go`
- `internal/app/runner_test.go`

## Baseline Antes Das Alteracoes

Arquivos salvos:

- `.codex/baseline-test-results.txt`
- `.codex/baseline-bench-results.txt`
- `.codex/baseline-e2e-summary.csv`

### Testes

- `go test ./...`: verde
- `go test ./... -bench=. -benchmem`: sem benchmarks existentes naquele momento, portanto sem baseline microbench comparavel

### End-to-end baseline

Cenario: `10k` eventos, `4 KiB`, `6` particoes, `snappy`

| formato | connector sec | throughput rec/s | connector RSS KB | objetos | size bytes |
|---|---:|---:|---:|---:|---:|
| parquet | 3.43 | 2915.45 | 119776 | 6 | 40270479 |
| avro | 3.40 | 2941.18 | 173784 | 6 | 42091172 |

## Implementacao Realizada

### 1. Batch/assembler mais leve

- `BatchWindow` agora carrega apenas metadados de lote: `topic`, offsets, contagem, bytes aproximados e afins
- o payload dos registros e serializado imediatamente em temp file, em vez de ficar retido em `[]LandingRecord`
- para `avro`, o fast path escreve incrementalmente e evita copia extra desnecessaria do payload
- para `parquet`, o codigo foi mantido seguro, mas ainda depende de buffering interno da biblioteca

### 2. Headers e metadata

- `include_headers=false` agora evita completamente a serializacao de headers
- `KafkaMessage` passou a carregar `HeadersJSON []byte` em vez de `map[string]string`
- o custo de ingest timestamp foi deslocado para o poll batch

### 3. Backpressure e commit safety

- `max_parallel_flushes` agora limita o numero de janelas seladas em voo, cobrindo encode temporario + upload + commit
- o coordinator coalesce commits contiguos por particao, sem quebrar ordenacao
- offset continua sendo commitado apenas apos persistencia/upload com sucesso
- falha de upload nao comita offset e libera recursos

### 4. Operacao e docs

- `temp_dir` foi formalizado em config
- exemplos e docs agora deixam claro que Landing Zone e raw append-only/WAL-like
- `avro` foi documentado como write path recomendado para captura barata
- `parquet` foi documentado como opcao analitica/compativel, nao fast path

## Resultados Depois Das Alteracoes

Arquivos salvos:

- `.codex/post-test-results.txt`
- `.codex/post-vet-results.txt`
- `.codex/post-bench-results.txt`
- `.codex/post-e2e-summary.csv`

### Validacao

- `gofmt`: executado nos arquivos Go
- `go vet ./...`: verde
- `go test ./...`: verde
- `go test ./... -bench=. -benchmem`: verde
- script end-to-end executado novamente com `10k` eventos
- rodada adicional `avro/snappy` com `100k` eventos executada para observar escalabilidade do fast path

### Microbenchmarks atuais

Nao havia benchmark baseline anterior equivalente. Estes numeros passam a ser o baseline de microbench daqui para frente.

| benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkNewKafkaMessageHeadersOff` | 17.70 | 0 | 0 |
| `BenchmarkNewKafkaMessageHeadersOn` | 641.6 | 112 | 2 |
| `BenchmarkStreamWriter` Avro | 2850 | 5035 | 1 |
| `BenchmarkStreamWriter` Parquet | 4904 | 1017 | 1 |

Leitura principal:

- o caminho sem headers agora e allocation-free
- manter headers ligados continua tendo custo perceptivel, mas controlado
- o writer Avro incremental ficou com custo simples e previsivel

### End-to-end comparativo

Cenario comparavel: `10k` eventos, `4 KiB`, `6` particoes, `snappy`

| formato | baseline sec | pos sec | delta tempo | baseline rec/s | pos rec/s | delta throughput | baseline RSS KB | pos RSS KB | delta RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| parquet | 3.43 | 4.41 | +28.57% | 2915.45 | 2267.57 | -22.22% | 119776 | 328132 | +173.95% |
| avro | 3.40 | 3.29 | -3.24% | 2941.18 | 3039.51 | +3.34% | 173784 | 184420 | +6.12% |

Delta de tamanho em disco:

- parquet: `40270479 -> 40224150` bytes, `-0.12%`
- avro: `42091172 -> 42055354` bytes, `-0.09%`

### Rodada adicional do fast path

Cenario: `100k` eventos, `4 KiB`, `6` particoes, `avro+snappy`

| formato | connector sec | throughput rec/s | connector RSS KB |
|---|---:|---:|---:|
| avro | 17.50 | 5714.29 | 527916 |

Leitura principal:

- o fast path `avro` manteve comportamento correto e lag final zero
- em escala maior, o throughput subiu para cerca de `5.7k rec/s`, indicando que o pipeline desacoplado com temp file conseguiu escalar melhor que no smoke de `10k`

## Interpretacao Dos Resultados

O resultado e misto, mas util:

- para o objetivo de Landing raw barato, o caminho `avro` melhorou levemente no teste comparavel e ganhou uma trilha mais previsivel para evolucao
- o maior ganho qualitativo foi arquitetural: o lote deixou de reter `[]LandingRecord` completo em memoria, o que reduz risco estrutural de explosao de RSS em cargas maiores
- `max_parallel_flushes` agora realmente controla memoria em voo, o que melhora previsibilidade operacional
- o caminho `parquet` piorou no cenario pequeno e continua sendo o principal gargalo de CPU/memoria; isso confirma que ele nao deve ser tratado como fast path de landing

## Testes Adicionados Ou Ajustados

- `include_headers=false` nao materializa headers
- commit coordinator nao comita fora de ordem
- commit coordinator coalesce janelas contiguas
- falha no upload nao comita offsets
- `max_parallel_flushes` limita concorrencia real
- assembler preserva `topic/partition/start_offset/end_offset`
- cleanup de temp file em `Abort`

## Riscos Remanescentes

- o writer Parquet ainda depende de buffering/chunking da biblioteca e segue caro para CPU/RSS
- o uso de temp file remove retencao do lote em heap, mas ainda ha custo de IO local e page cache do sistema
- `encodeLimiter` por mensagem e conservador; ele protege concorrencia, mas pode limitar throughput em cenarios futuros se ficar abaixo do ponto ideal
- o teste comparavel de `10k` e pequeno e tem ruido; a interpretacao deve olhar mais para tendencia arquitetural do que para diferencas marginais de poucos pontos percentuais

## Proximos Passos Recomendados

1. Tornar `avro` o default operacional para landing raw e tratar `parquet` apenas como modo compativel/analitico.
2. Atacar o caminho Parquet separadamente, com expectativa de menor retorno, ja que ele nao e o fast path desejado.
3. Evoluir o temp-file path para upload streaming/multipart quando o sink permitir, reduzindo ainda mais copia e tempo ate persistencia remota.
4. Medir memoria de processo e GC com mais profundidade em cargas maiores, por exemplo `100k`, `300k` e `500k`, usando o mesmo harness.
5. Refinar o limite de concorrencia entre `max_parallel_flushes`, `max_parallel_encodes` e `max_parallel_uploads` com foco em throughput sustentado e menor RSS.
6. Adicionar benchmark dedicado do assembler incremental para medir `B/op` e `allocs/op` do hot path diretamente, sem envolver o sink remoto.

## Conclusao

O runtime ficou mais alinhado com a proposta de Landing Zone raw/WAL-like: menos estado em memoria, offset safety preservada, backpressure mais real e caminho `avro` mais coerente como fast path. O principal trabalho restante nao e "mais paralelismo" de forma cega; e continuar removendo custo do write path, especialmente onde ainda existe buffering pesado ou dependencia de bibliotecas mais caras, com prioridade clara para o fast path raw.
