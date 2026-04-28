# Performance Deep Dive (2026-04-28)

## Objetivo

Documentar em profundidade:

- as ultimas alteracoes implementadas no runtime
- por que elas melhoraram o resultado de performance
- os resultados medidos
- as proximas oportunidades tecnicas com maior potencial de ganho

Escopo do workload analisado:

- Kafka/Redpanda -> Landing (MinIO), formato `avro`
- `compression: null`
- upload streaming direto (sem temp file local)
- payload pseudoaleatorio de `8 KiB` (baixa compressibilidade)
- `6` particoes
- `include_headers: true`
- `include_key: true`
- perfil operacional principal: `2:2:2`

---

## 1) Alteracoes recentes implementadas

### 1.1 Controle de concorrencia no hot path (Runner)

Arquivos:

- `internal/app/runner.go`
- `internal/app/runner_test.go`

Mudancas principais:

- aplicacao real de `max_parallel_uploads` no ciclo de flush/upload
- manutencao de `max_parallel_flushes` como limite de janelas em voo
- `max_parallel_encodes` preservado com limitacao por mensagem
- commit coordinator inalterado em semantica de seguranca (commit apenas apos upload bem-sucedido)

Detalhe tecnico importante:

- foi testada uma variante de encode limitado por stream inteiro
- com `6` particoes e `max_parallel_encodes=2`, essa abordagem introduziu starvation entre particoes e piorou estabilidade
- decisao final: manter encode por mensagem (mais previsivel neste ambiente), e limitar upload no momento de fechamento da janela

Impacto arquitetural:

- melhor isolamento de backpressure por etapa
- menos risco de deadlock/starvation entre particoes
- limite de upload agora corresponde ao knob operacional definido em config

### 1.2 Auto-tuning por perfil da maquina (CPU/RAM)

Arquivos:

- `internal/config/config.go`
- `internal/config/config_test.go`
- `configs/orders.example.yaml`
- `configs/orders.minio.example.yaml`
- `README.md`

Mudancas principais:

- introducao de detecao de host profile (CPU + MemTotal via `/proc/meminfo`)
- default automatico para:
  - `kafka.poll_records`
  - `kafka.fetch_max_bytes`
  - `kafka.fetch_max_partition_bytes`
  - `kafka.fetch_min_bytes`
  - `kafka.fetch_max_wait`
  - `runtime.max_parallel_flushes`
  - `runtime.max_parallel_encodes`
  - `runtime.max_parallel_uploads`
  - `runtime.flush_queue_size`
  - `runtime.partition_queue_size`
- regra de override mantida:
  - se o campo vier explicitamente no YAML, ele prevalece
  - auto-tuning atua somente quando valor configurado e `0` (ou vazio, conforme campo)

Impacto arquitetural:

- reduz dependencia de tuning manual inicial
- acelera bootstrap em ambientes diferentes sem quebrar compatibilidade
- mantem previsibilidade operacional porque override manual continua soberano

### 1.3 Otimizacoes de alocacao no caminho de headers/payload metadata

Arquivos:

- `internal/adapters/source/kafka/source.go`
- `internal/batch/assembler.go`

Mudancas principais:

- serializacao de headers sem `fmt.Fprintf` no hot path para escapes de controle
- uso de escape manual em bytes para reduzir custo por header
- no assembler, conversao de `[]byte` para string imutavel sem copia extra para `headers_json`

Impacto arquitetural:

- menor pressao de alocacao por mensagem
- menor custo de CPU na etapa de montagem do registro
- ganho incremental, especialmente em cenarios com headers habilitados

---

## 2) Validacao funcional e de regressao

Executado apos as alteracoes:

- `go test ./...`
- `go vet ./...`
- `go test ./... -bench=. -benchmem`

Status:

- todos os testes passaram
- sem quebra da semantica de commit safety
- sem regressao funcional no fluxo de upload/commit

---

## 3) Resultados medidos (antes vs depois)

## 3.1 100k (null, streaming, 2:2:2)

Comparacao com ultimo 100k valido anterior:

- antes: `46.55s`, `2148.23 rec/s`, RSS `690752 KB`
- depois: `44.71s`, `2236.64 rec/s`, RSS `791528 KB`

Delta:

- tempo: `-3.95%`
- throughput: `+4.12%`
- RSS: `+14.59%`

Leitura:

- ganho moderado em throughput/tempo para carga curta
- custo em pico de RSS no run especifico
- nao houve lag residual

## 3.2 500k (null, streaming, 2:2:2) - comparacao principal

Baseline anterior:

- arquivo: `.codex/diag-streaming-null-500k-part16-20260427T233215Z/summary.csv`
- `connector_elapsed_sec=206.20`
- `throughput=2424.83 rec/s`
- `connector_maxrss_kb=800452`

Reteste atual:

- arquivo: `.codex/retest-500k-diag-20260428T001539Z/summary.csv`
- `connector_elapsed_sec=145.12`
- `throughput=3445.42 rec/s`
- `connector_maxrss_kb=820116`

Delta:

- tempo total: `-29.62%` (`206.20s -> 145.12s`)
- throughput: `+42.09%` (`2424.83 -> 3445.42 rec/s`)
- RSS: `+2.46%`
- lag final: `0` em ambos
- volume final: igual (`4165983764 bytes`)

Leitura:

- ganho forte e consistente na carga longa
- memoria ficou praticamente no mesmo patamar relativo
- melhora de tempo nao veio de compressao/tamanho (volume final igual), mas de eficiencia de pipeline

## 3.3 Analise por batch (500k)

Fonte:

- `null.connector.log` do baseline e do reteste atual

Comparacao (batches cheios de 10k):

- `encode_mean`: `20.32s -> 13.66s` (`-32.8%`)
- `upload_mean`: `21.58s -> 14.24s` (`-34.0%`)
- `time_to_commit_mean`: `1.47s -> 0.67s` (`-54.5%`)
- `commit_mean`: `9.97ms -> 4.77ms` (`-52.2%`, irrelevante no total)

Leitura:

- gargalo dominante permanece em `encode + upload`
- commit nao e gargalo
- melhora veio de melhor acoplamento entre fechamento de janela, upload e coordenacao

---

## 4) Por que as alteracoes melhoraram os resultados

### 4.1 Backpressure mais correto no ponto de upload

Antes, parte do controle efetivo de concorrencia estava difuso entre semaforos e temporalidade de abertura/fechamento de stream.  
Agora o limite de upload e aplicado no fechamento da janela (quando o upload efetivamente entra na fase critica), o que:

- reduz competicao inutil entre particoes
- estabiliza o numero de uploads ativos
- melhora a previsibilidade de memoria e latencia por batch

### 4.2 Evitamos starvation acidental entre particoes

A tentativa de segurar encode por stream inteiro mostrou efeito colateral em ambiente com 6 particoes e limites baixos (`2`).  
Ao voltar encode para granularidade por mensagem, preservamos fairness entre particoes e removemos pontos de bloqueio prolongado.

### 4.3 Menos custo por mensagem no caminho de headers

Remover formatacao pesada no escape e evitar copia extra no `headers_json` reduz overhead unitario.  
Em workloads grandes, esse custo unitario aparece de forma acumulada no `encode_duration`.

### 4.4 Defaults mais alinhados ao host real

Com auto-tuning (quando knobs estao em `0`), o runtime evita defaults genericos subotimizados para a maquina local.  
Isso reduz tempo ate um perfil operacional aceitavel sem sacrificar controle manual.

---

## 5) Gargalo atual com evidencias

## 5.1 Gargalo principal hoje

No reteste de 500k, a etapa dominante ainda e:

- `encode + upload` por janela

Evidencias:

- `commit_duration` medio em milissegundos
- `upload_duration` e `encode_duration` em dezenas de segundos por batch
- CPU global do processo ainda baixa no `time -v` (indicando espera de I/O e/ou pipeline-bound, nao CPU-bound puro)

## 5.2 Gargalo de encerramento (tail)

No run de 500k:

- 48 batches cheios processam 480k registros com tempos medios muito melhores
- os ultimos 20k (6 batches parciais) ficaram caros (`~33-34s` por batch parcial)

Metricas observadas:

- diferenca entre fim do ultimo batch cheio e `pipeline finished`: ~`33.7s`
- no baseline antigo esse tail foi ~`40.5s`

Leitura:

- ha um custo de drenagem/idle flush no fim do run que distorce o wall time total em execucao finita de benchmark
- esse custo existe mesmo quando o backlog principal ja foi drenado

---

## 6) Proximas oportunidades (detalhamento tecnico)

## 6.1 Oportunidade 1 - Modo `drain and stop` para execucao finita

Hipotese de maior ganho imediato.

Problema atual:

- a logica de termino usa politica de idle polling generica
- em benchmark finito (produtor para, backlog zera), ainda existe janela de espera/encerramento parcial que adiciona cauda

Proposta tecnica:

- introduzir um modo explicito de execucao finita, por exemplo:
  - `runtime.drain_mode: finite` (ou flag equivalente)
- comportamento:
  1. detectar ausencia de novas mensagens por N polls
  2. sinalizar drain global
  3. forcar seal dos assemblers ativos sem aguardar `max_duration`
  4. bloquear entrada nova
  5. aguardar finalize/upload/commit
  6. encerrar imediatamente apos todos os partitions workers ficarem quiescentes

Invariantes obrigatorias:

- commit safety preservada
- nenhuma janela com upload falho pode commitar
- ordem de commit por particao preservada

Potencial estimado (benchmark atual):

- remover ~`33.7s` de cauda em `145.12s` pode levar para ~`111s`
- throughput efetivo estimado nessa hipotese: ~`4.5k rec/s`
- ganho relativo potencial: ~`+30%` adicional sobre o run atual

Risco:

- termino antecipado incorreto se houver chegada tardia real de mensagens
- mitigacao: ativar apenas em modo finito de benchmark/job com criterio de drain robusto

## 6.2 Oportunidade 2 - Tuning de source (`poll_records` e `fetch_*`)

Objetivo:

- alimentar melhor o pipeline durante backlog
- reduzir overhead de polls pequenos e ciclos de wake-up

Contexto tecnico:

- `poll_records` controla quantos registros sao drenados por poll
- `fetch_min_bytes` + `fetch_max_wait` controlam agregacao no broker antes de responder
- `fetch_max_bytes` e `fetch_max_partition_bytes` limitam tamanho dos lotes de resposta

Direcao de tuning:

- aumentar `poll_records` no backlog (ex: `2000`, `4000`)
- elevar `fetch_min_bytes` para evitar respostas muito pequenas
- ajustar `fetch_max_wait` para equilibrar throughput vs latencia
- manter limites de fetch compatíveis com payload de `8 KiB` e 6 particoes

Plano de teste recomendado:

Fase A (fixar 2:2:2 + null):

1. `poll_records`: `1000`, `2000`, `4000`
2. para cada ponto, variar:
   - `fetch_min_bytes`: `256 KiB`, `1 MiB`, `2 MiB`
   - `fetch_max_wait`: `50ms`, `100ms`, `200ms`

Metricas de decisao:

- tempo total
- throughput
- variancia de `encode/upload_duration` por batch
- impacto em RSS

Risco:

- fetch grande demais pode aumentar burst de memoria
- mitigacao: observar `heap_alloc`, `heap_sys`, RSS e page cache em paralelo

## 6.3 Oportunidade 3 - Separar `upload_active_time` vs `stream_open_time`

Problema atual de observabilidade:

- `upload_duration` hoje mede o tempo total da chamada de upload streaming
- como o upload recebe dados de `io.Pipe`, esse tempo inclui:
  - espera enquanto encoder produz bytes
  - transmissao efetiva para MinIO

Consequencia:

- fica dificil dizer se o gargalo dominante e:
  - escrita Avro (produtor do stream)
  - rede/MinIO (consumidor do stream)

Proposta tecnica:

1. Instrumentar `countingReader` no caminho do sink para medir:
   - `first_byte_at`
   - `last_byte_at`
   - `bytes_sent`
2. Registrar no log por batch:
   - `stream_open_time` = inicio do upload -> fim
   - `upload_active_time` = `last_byte_at - first_byte_at`
   - `upload_wait_time` = `stream_open_time - upload_active_time`
3. Expor taxas:
   - `upload_active_bytes_per_sec`
   - `pipeline_producer_bytes_per_sec` (lado writer)

Interpretação esperada:

- `upload_wait_time` alto => produtor (encode) nao acompanha
- `upload_active_time` alto com bytes/s baixo => gargalo em MinIO/rede/disco

Resultado pratico:

- permite decidir com evidencia se o proximo passo deve atacar writer Avro, sink MinIO ou source tuning

---

## 7) Evolucao temporal dos testes 500k

Arquivos gerados:

- grafico geral:
  - `.codex/charts/500k-time-evolution-all.svg`
- grafico foco `2:2:2`:
  - `.codex/charts/500k-time-evolution-2x2x2.svg`
- dados consolidados:
  - `.codex/charts/500k-time-evolution-data.csv`

Leitura consolidada:

- tendencia de queda forte do tempo ao longo das iteracoes
- no perfil operacional `2:2:2`, o melhor resultado atual e `145.12s`
- progresso relevante sem alterar semantica de seguranca de offsets

---

## 8) Recomendacao objetiva de proxima iteracao

Ordem sugerida para maximizar ganho rapido:

1. implementar modo `drain and stop` para execucao finita
2. medir novamente 500k e validar ganho de cauda
3. em seguida executar matriz de source tuning (`poll_records/fetch_*`)
4. adicionar separacao de metricas `upload_active_time` vs `stream_open_time`
5. reavaliar gargalo real com nova instrumentacao antes de mexer em paralelismo

Critério de sucesso da proxima rodada:

- reduzir tempo de 500k para faixa proxima de `~110-120s`
- manter `lag=0`
- manter commit safety
- manter RSS em faixa operacional para host de 8 GB
