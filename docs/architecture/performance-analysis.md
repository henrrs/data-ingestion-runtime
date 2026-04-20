# Analise de Performance

Este documento consolida o baseline de performance observado no teste de estresse mais recente do conector e lista os proximos passos recomendados para melhorar throughput, uso de memoria e previsibilidade operacional.

## Objetivo

Responder tres perguntas:

1. O conector consegue drenar um backlog grande com payload pouco compressivel?
2. Onde estao os gargalos mais provaveis no pipeline atual?
3. Qual a sequencia mais segura de ajustes para melhorar performance?

## Cenario testado

Data do teste valido:

- `2026-04-20` em UTC

Carga gerada:

- `1.000.000` eventos
- `8 KiB` por evento
- payload `pseudo-random` deterministico
- baixa compressibilidade
- `6` particoes Kafka

Configuracao relevante do teste:

- topico: `orders-stress-1m-8kb-rand-6p-20260419-1`
- `batch.max_records: 10000`
- `batch.max_bytes: 104857600`
- `batch.max_duration: 10m`
- `output.parquet_compression: zstd`
- `output.include_headers: true`
- `output.include_key: true`
- sink: MinIO local
- source: Kafka local em Redpanda

Observacao importante:

- a primeira tentativa nao foi considerada valida para baseline porque o broker local caiu no meio da carga quando estava configurado com apenas `1 vCPU` e `1 GiB`
- o teste valido foi repetido com o broker local recriado com `2 vCPU` e `2 GiB`

## Resultado consolidado

### Producao Kafka

- `1.000.000` eventos produzidos com sucesso
- payload bruto total: `7812.50 MiB`
- tempo total do produtor: `3m00.619s`
- throughput medio do produtor: `5537 eventos/s`
- throughput medio bruto do produtor: `43.25 MiB/s`
- pico de memoria do produtor: `136.95 MiB`

### Consumo e escrita do conector

- `1.000.000` eventos consumidos com sucesso
- tempo total do conector: `254.99s`
- throughput medio do conector: `3921.72 eventos/s`
- throughput medio bruto equivalente: `30.64 MiB/s`
- pico de memoria do conector: `766.58 MiB`
- `100` flushes executados
- `100` arquivos Parquet gerados
- `10000` registros por arquivo
- tamanho observado por arquivo: tipicamente entre `70 MiB` e `72 MiB`
- volume final no MinIO: aproximadamente `6.9 GiB`
- consumer group final com `TOTAL-LAG 0`

### Distribuicao nas particoes

A distribuicao ficou equilibrada entre as `6` particoes:

- particao `0`: `166546`
- particao `1`: `166720`
- particao `2`: `166579`
- particao `3`: `166879`
- particao `4`: `167070`
- particao `5`: `166206`

### Leitura rapida do resultado

- o pipeline funcionou corretamente de ponta a ponta
- o conector sustentou a drenagem completa do backlog sem perder offsets
- o conector foi mais lento que o produtor neste cenario
- a compressao ZSTD ajudou pouco, o que e coerente com um payload desenhado para ser pouco compressivel

## O que os numeros indicam

### 1. O batch foi limitado por `max_records`, nao por `max_bytes`

Os logs do flush mostraram repetidamente:

- `records: 10000`
- `bytes_approx: 82410000`

Ou seja:

- cada lote tinha aproximadamente `82.41 MB`
- o limite de `100 MiB` nao foi atingido
- o gatilho real de flush foi `max_records`

Conclusao pratica:

- hoje, aumentar `max_bytes` sozinho nao deve mudar esse perfil
- qualquer tuning de lote precisa comecar por `max_records`

### 2. O uso de memoria esta alto para um lote de ~82 MB

O processo do conector atingiu aproximadamente `766 MiB` de RSS para lotes que, em bytes aproximados, ficaram perto de `82 MB`.

Isso sugere forte overhead de alocacao e copias intermediarias. Essa leitura e consistente com o codigo atual em:

- `internal/batch/assembler.go`
- `internal/adapters/sink/parquetutil/writer.go`
- `internal/adapters/sink/minio/sink.go`

### 3. O pipeline atual e fortemente serial

Em `internal/app/runner.go`, o fluxo efetivo e:

1. poll do Kafka
2. montagem do lote em memoria
3. escrita completa do Parquet
4. upload completo do objeto
5. commit dos offsets
6. volta ao poll

Durante o flush, o loop principal nao continua consumindo nem preparando o proximo lote. Isso reduz paralelismo e faz throughput depender do trecho mais lento da etapa de flush.

### 4. O ganho de compressao foi modesto

O payload bruto do teste foi de `7812.50 MiB`, e o volume final materializado ficou em torno de `6.9 GiB`.

Em outras palavras:

- houve alguma compressao
- mas o ganho foi relativamente pequeno
- esse comportamento e esperado para payload pseudo-random e de baixa compressibilidade

Conclusao pratica:

- aumentar nivel de compressao do ZSTD provavelmente vai custar CPU demais para pouco beneficio de tamanho
- vale testar `snappy` ou `uncompressed` como comparativo de throughput

## Gargalos mais provaveis no codigo atual

As observacoes abaixo sao inferencias baseadas no codigo e nos resultados do teste.

### 1. Copias repetidas do payload na montagem do lote

Em `internal/batch/assembler.go`:

- `json.Valid(raw)` percorre todo o payload
- `string(raw)` cria uma nova string para o payload JSON
- `json.Marshal(msg.Headers)` gera nova alocacao para headers
- `string(msg.Key)` gera nova alocacao para a chave

Para `1.000.000` eventos de `8 KiB`, isso representa custo relevante de CPU e GC.

### 2. Conversao completa para um segundo slice antes de gravar o Parquet

Em `internal/adapters/sink/parquetutil/writer.go`:

- `WriteRecords` recebe `[]model.LandingRecord`
- depois cria outro slice completo via `convert(...)`
- essa conversao e feita a cada flush

Isso duplica trabalho e memoria para cada lote.

### 3. Bufferizacao completa do arquivo antes do upload

Em `internal/adapters/sink/minio/sink.go`:

- o Parquet inteiro e gerado em um `bytes.Buffer`
- so depois o `PutObject` envia o objeto ao MinIO

Esse desenho impede sobrepor geracao e upload e aumenta o pico de memoria.

### 4. Escrita e commit totalmente sincronizados no caminho quente

Em `internal/app/runner.go`:

- `flush()` escreve no sink
- depois faz commit
- so entao volta a consumir

Esse desenho simplifica semantica operacional, mas limita throughput.

## Proximos passos recomendados

### Prioridade 0: estabilizar o ambiente de benchmark

Antes de otimizar o conector, o ambiente local precisa ser suficiente para nao mascarar o resultado.

Acoes:

- manter um perfil de benchmark local com Redpanda em pelo menos `2 vCPU` e `2 GiB`
- separar um `docker-compose` ou profile especifico para testes de carga
- registrar os parametros do ambiente junto do resultado

Impacto esperado:

- melhora a confiabilidade das comparacoes entre execucoes

### Prioridade 1: reduzir picos de memoria e copias desnecessarias

Esta e a frente com melhor relacao risco/beneficio.

Acoes:

- eliminar a conversao `[]LandingRecord -> []landingRecordZstd` no `parquetutil`
- escrever Parquet de forma incremental, sem montar um segundo slice completo
- trocar o `bytes.Buffer` por streaming com `io.Pipe` entre writer Parquet e `PutObject`
- revisar se `payload_json` precisa mesmo passar por `json.Valid` em todos os eventos
- tornar `include_headers` e `include_key` opcionais por workload real e desabilitar quando nao forem necessarios

Impacto esperado:

- reducao forte do RSS
- menos pressao de GC
- mais throughput por flush

### Prioridade 2: aumentar paralelismo do pipeline

Hoje o pipeline e essencialmente monolitico e sincronizado.

Acoes:

- separar consumo, serializacao e upload em estagios independentes
- permitir que um lote seja comprimido e enviado enquanto o proximo ja esta sendo montado
- considerar um pool limitado de workers de flush
- manter commit apenas apos confirmacao segura da persistencia do lote

Impacto esperado:

- melhor uso de CPU
- menor tempo ocioso entre polls
- throughput maior em topicos com varias particoes

Risco:

- aumenta complexidade de ordenacao e checkpoint
- exige cuidado para nao comprometer a semantica de commit atual

### Prioridade 3: revisar o contrato do payload

Hoje o payload vai para o lote como `string` em `PayloadJSON`.

Acoes:

- avaliar armazenar o payload como `[]byte` ou `BINARY` em vez de `string`
- tratar validacao JSON como responsabilidade opcional, nao obrigatoria do caminho quente
- se essa mudanca for adotada, registrar a alteracao por ADR porque impacta schema e consumo downstream

Impacto esperado:

- menos copias
- menor custo de CPU
- caminho de ingestao mais proximo do payload bruto original

### Prioridade 4: tunar codec e tamanho de batch com dados reais

Com payload pouco compressivel, compressao mais agressiva tende a piorar custo/beneficio.

Acoes:

- comparar `zstd`, `snappy` e `uncompressed`
- repetir benchmark com `max_records` em `5000`, `10000` e `15000`
- medir o efeito de `include_headers=false` e `include_key=false`
- repetir com payloads mais proximos do trafego real de producao

Impacto esperado:

- encontrar o melhor ponto entre CPU, memoria e tamanho final

## Ordem sugerida de execucao

Sequencia recomendada para implementar sem perder rastreabilidade:

1. Criar perfil de benchmark estavel
2. Implementar escrita do sink sem `bytes.Buffer` completo
3. Remover a conversao extra no `parquetutil`
4. Medir novamente o mesmo cenario `1M x 8 KiB x 6 particoes`
5. So depois experimentar paralelismo de flush
6. Por ultimo, revisar schema do payload se ainda houver gargalo relevante

## Benchmark minimo da proxima rodada

Para a proxima iteracao, vale repetir exatamente este conjunto:

- `1.000.000` eventos
- `8 KiB`
- payload pseudo-random
- `6` particoes
- comparar `zstd` vs `snappy`
- comparar implementacao atual vs sink com streaming

Metricas minimas a registrar:

- eventos por segundo
- MiB/s de payload bruto
- RSS maximo
- numero de arquivos
- tamanho total materializado
- lag final
- tempo total do conector

## Conclusao

O conector esta funcional e consistente sob carga pesada, mas o baseline mostra um desenho ainda caro em memoria e excessivamente serial para workloads grandes e pouco compressiveis.

O melhor proximo passo nao e aumentar batch nem apertar mais a compressao. O melhor proximo passo e reduzir copias e bufferizacao no caminho quente, e depois introduzir paralelismo controlado.
