# Fluxo Atual do Conector

Este documento descreve o fluxo atual do runtime apos a convergencia para o write path `avro`.

## Visao Geral

O conector:

1. abre um consumer Kafka com commit manual
2. faz `poll` em blocos
3. roteia mensagens para workers por particao
4. cada worker monta a janela de batch
5. cada mensagem e codificada incrementalmente em Avro OCF para um stream em memoria (`io.Pipe`)
6. o stream e enviado diretamente ao sink (sem arquivo temporario local)
7. o commit ocorre somente depois do upload bem-sucedido

## Diagrama

```mermaid
flowchart LR
    A["Kafka / Redpanda"] --> B["Kafka Source"]
    B --> C["Partition Router"]
    C --> D["Partition Worker"]
    D --> E["Batch Assembler"]
    E --> F["Avro Stream Writer"]
    F --> G["Streaming Pipe"]
    G --> H["Sink Upload"]
    H --> I["MinIO / ADLS"]
    I --> J["Commit Coordinator"]
    J --> K["Kafka Offset Commit"]
```

## Semantica

- landing raw, append-only e WAL-like
- sem deduplicacao
- sem parsing pesado do payload
- sem transformacao semantica para Bronze/Delta
- commit somente apos persistencia remota confirmada
- ordem de commit preservada por particao

## Controles de Concorrencia

- `runtime.max_parallel_flushes`
  limita quantas janelas podem ficar em voo do fechamento ate o commit
- `runtime.max_parallel_encodes`
  limita a concorrencia da etapa quente de encode local
- `runtime.max_parallel_uploads`
  define quantos uploads podem acontecer em paralelo
- `runtime.partition_queue_size`
  define o buffer por particao antes de aplicar backpressure

## Observabilidade

O conector registra por janela:

- `records`
- `approx_input_bytes`
- `stream_size`
- `records_per_sec`
- `upload_active_bytes_per_sec`
- `encode_duration`
- `upload_duration`
- `upload_active_duration`
- `upload_wait_for_first_byte`
- `commit_duration`
- `total_batch_duration`
- `time_to_commit`
- `format`
- `compression`
- `file_path`

Ao final da execucao ele tambem registra:

- `heap_alloc_bytes`
- `heap_inuse_bytes`
- `heap_sys_bytes`
- `total_alloc_bytes`
- `alloc_delta_bytes`
- `alloc_rate_bytes_per_sec`
- `gc_cycles`
- `gc_pause_total_ns`
