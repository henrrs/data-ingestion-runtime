# Landing Connector

Conector batch para consumir dados de um topico Kafka e gravar arquivos Parquet com compressao ZSTD em uma landing zone object storage.

## Caracteristicas do MVP

- Consumo Kafka com commit manual
- Batch limitado por tempo, quantidade de registros e bytes aproximados
- Escrita em Parquet com payload bruto preservado
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

## Arquitetura

As decisoes arquiteturais ficam documentadas em [docs/architecture/README.md](docs/architecture/README.md).
O baseline mais recente de performance fica em [docs/architecture/performance-analysis.md](docs/architecture/performance-analysis.md).
