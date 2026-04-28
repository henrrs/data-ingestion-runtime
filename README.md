# Landing Connector

Conector batch para consumir dados de um topico Kafka e gravar arquivos raw append-only em uma landing zone object storage.

## Papel Da Landing Zone

A Landing Zone deste runtime funciona como uma camada raw, imutavel e WAL-like:

- captura o dado do Kafka o mais fiel possivel
- nao faz deduplicacao
- nao faz parsing pesado do payload
- nao faz transformacao semantica para modelo analitico
- so comita offsets depois de persistir o arquivo com sucesso

A materializacao para Bronze/Delta deve acontecer depois, fora deste caminho quente.

## Caracteristicas

- Consumo Kafka com commit manual
- Batch limitado por tempo, quantidade de registros e bytes aproximados
- Escrita em Avro OCF com payload bruto preservado
- Colunas tecnicas do Kafka para auditoria e replay
- Manifest simplificado em logs estruturados
- Preparado para execucao em `Kubernetes CronJob`
- Arquitetura extensivel com `Source` e `Sink` plugaveis

## Estrutura

- `cmd/landing-connector`: ponto de entrada
- `internal/core`: portas e factories
- `internal/adapters`: adapters concretos de origem, destino e serializacao
- `internal/config`: carga e validacao de configuracao
- `internal/app`: orquestracao da execucao batch
- `internal/batch`: montagem do lote
- `internal/model`: contratos tecnicos do lote
- `docs/architecture`: decisoes arquiteturais e guias

## Exemplo de execucao

```powershell
landing-connector.exe -config .\configs\orders.yaml
```

## Configuracao

Veja o exemplo em [configs/orders.example.yaml](configs/orders.example.yaml).
Para teste local com MinIO, veja [configs/orders.minio.example.yaml](configs/orders.minio.example.yaml).
Para ambiente local de integracao, veja [deploy/docker-compose.local.yml](deploy/docker-compose.local.yml).
Ao subir o ambiente local, o Redpanda Console fica em `http://localhost:8080` e o MinIO Console em `http://localhost:9001`.

## Recomendacao De Formato

- `avro` e o formato operacional padrao da landing raw
- o runtime foi simplificado para um unico write path, com foco em throughput, menor RSS e menor pressao de GC
- a Bronze/Delta deve ser materializada em uma etapa posterior, fora deste runtime

Os knobs principais de throughput ficam em `runtime`:

- `max_parallel_flushes`: numero maximo de batches em voo entre fechamento da janela, upload e commit
- `max_parallel_encodes`: limite de concorrencia da etapa de encode local no caminho quente
- `max_parallel_uploads`: paralelismo da etapa de envio ao sink
- `flush_queue_size`: buffer entre fechamento da janela e upload
- `partition_queue_size`: buffer de entrada por particao antes de aplicar backpressure
- `execution_mode`: `auto|finite|continuous`
- `autotune_mode`: `auto|off`
- `autotune_interval`: janela de decisao do autotune em runtime
- `autotune_max_workers`: teto dinamico para `flush/encode/upload` durante exploracao
- `autotune_poll_max`: teto dinamico para `poll_records` em runtime
- `drain_idle_poll_count`: limite de polls ociosos para encerrar run finito apos drenar backlog

Auto-tuning operacional:

- quando `poll_records`, `fetch_*`, `max_parallel_*`, `flush_queue_size` ou `partition_queue_size` estao em `0`, o runtime aplica defaults automaticos com base no perfil da maquina (CPU/RAM)
- `execution_mode: auto` resolve para `finite` quando `run_timeout > 0` e para `continuous` quando `run_timeout = 0`
- `autotune_mode: auto` ativa controle adaptativo em duas fases:
  - fase source: ajusta `poll_records` efetivo
  - fase workers: explora `flush/encode/upload` com rollback automatico
- o teto de workers tambem e ajustado dinamicamente pelo numero de particoes ativas observadas na origem (ate `autotune_max_workers`)
- se um valor for informado explicitamente no YAML, ele sempre prevalece

Para maquina local pequena, como 8 GB de RAM e SSD compartilhado com Redpanda e MinIO, o ponto operacional inicial recomendado e:

```yaml
runtime:
  max_parallel_flushes: 2
  max_parallel_encodes: 2
  max_parallel_uploads: 2
```

Compressao:

- `null`: default operacional do conector para landing raw, priorizando menor custo de CPU no caminho quente
- `snappy`: opcao para cenarios onde o payload tenha compressibilidade real e o gargalo principal seja I/O

Kafka:

- `kafka.poll_records`: quantidade maxima de registros drenados por chamada de poll
- `kafka.fetch_max_bytes`, `kafka.fetch_max_partition_bytes`, `kafka.fetch_min_bytes`, `kafka.fetch_max_wait`: knobs opcionais para ajustar fetch sem mudar a semantica de commit
- usar `0` nesses campos habilita auto-tuning do perfil da maquina

Upload:

- o conector nao materializa arquivo temporario local antes do upload
- o commit continua acontecendo somente apos upload concluido com sucesso
- logs por batch incluem metricas separadas de upload:
  - `upload_stream_open_duration`
  - `upload_active_duration`
  - `upload_wait_for_first_byte`
  - `upload_tail_finalize_duration`

## Arquitetura

As decisoes arquiteturais ficam documentadas em [docs/architecture/README.md](docs/architecture/README.md).
O baseline mais recente de performance fica em [docs/architecture/performance-analysis.md](docs/architecture/performance-analysis.md).
O plano de implementacao das otimizacoes fica em [docs/architecture/implementation-plan-performance.md](docs/architecture/implementation-plan-performance.md).
Para rodar comparativos locais do write path Avro, use [scripts/bench/run-local-compression-matrix.sh](scripts/bench/run-local-compression-matrix.sh) e [scripts/bench/run-avro-e2e-matrix.sh](scripts/bench/run-avro-e2e-matrix.sh).
