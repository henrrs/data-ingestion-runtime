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
- `temp_dir`: diretorio dos arquivos temporarios usados antes do upload

## Arquitetura

As decisoes arquiteturais ficam documentadas em [docs/architecture/README.md](docs/architecture/README.md).
O baseline mais recente de performance fica em [docs/architecture/performance-analysis.md](docs/architecture/performance-analysis.md).
O plano de implementacao das otimizacoes fica em [docs/architecture/implementation-plan-performance.md](docs/architecture/implementation-plan-performance.md).
Para rodar comparativos locais do write path Avro, use [scripts/bench/run-local-compression-matrix.sh](scripts/bench/run-local-compression-matrix.sh) e [scripts/bench/run-avro-e2e-matrix.sh](scripts/bench/run-avro-e2e-matrix.sh).
