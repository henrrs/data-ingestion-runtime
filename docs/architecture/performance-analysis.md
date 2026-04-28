# Analise de Performance

Este documento consolida os testes de estresse executados no conector, registra os resultados mais relevantes e resume os aprendizados que devem orientar a proxima rodada de otimizacao.

## Objetivo

Responder quatro perguntas:

1. O conector consegue drenar backlog grande com payload pouco compressivel?
2. O que mudou depois das melhorias recentes no pipeline?
3. Qual compressao entrega o melhor equilibrio entre throughput, memoria e tamanho final?
4. Quais sao os proximos gargalos reais a atacar?

## Ambiente e Carga

Ambiente local usado nos testes validos:

- data principal dos testes: `2026-04-20`
- Redpanda local com pelo menos `2 vCPU` e `2 GiB`
- MinIO local como sink
- topicos com `6` particoes

Carga padrao usada para comparacao:

- payload `pseudo-random` deterministico
- baixa compressibilidade
- `8 KiB` por evento
- `batch.max_records: 10000`
- `batch.max_bytes: 104857600`
- `batch.max_duration: 10m`
- `output.include_headers: true`
- `output.include_key: true`

Observacao operacional importante:

- durante a bateria maior de comparacao por codec, o host local saturou disco ao tentar materializar multiplos cenarios grandes em sequencia
- por isso a comparacao completa entre codecs foi consolidada em `500.000` eventos, e nao em `1.000.000`, para manter repetibilidade sem contaminar o resultado com falha do ambiente

## Linha do Tempo dos Testes

### 1. Baseline original antes das melhorias

Este foi o baseline documentado antes da refatoracao do pipeline.

Cenario:

- `1.000.000` eventos
- `8 KiB`
- `6` particoes
- compressao `zstd`

Resultado:

- produtor: `3m00.619s`, `5537 ev/s`, pico `136.95 MiB`
- conector: `254.99s`, `3921.72 ev/s`, pico `766.58 MiB`
- volume final no MinIO: aproximadamente `6.9 GiB`
- `100` arquivos Parquet
- lag final `0`

Leitura:

- o pipeline drenava corretamente o backlog
- o caminho ainda era fortemente serial
- o uso de memoria ja era alto, mas ainda bem menor que nas primeiras tentativas de refatoracao

### 2. Revalidacao apos as melhorias do pipeline

Depois da implementacao de:

- upload com tamanho conhecido em arquivo temporario
- flush assincrono controlado por particao
- limite global de concorrencia
- `pprof` opcional e tooling de benchmark

foi executado novo teste grande com `zstd`.

Cenario:

- `1.000.000` eventos
- `8 KiB`
- `6` particoes
- compressao `zstd`

Resultado:

- produtor: `154.60s`, pico `119780 KB`
- conector: `236.81s`, pico `3130832 KB`
- throughput do conector: aproximadamente `4223 ev/s`
- volume final no MinIO: `7376259355` bytes, cerca de `6.87 GiB`
- `102` arquivos Parquet

Leitura:

- houve melhora de throughput em relacao ao baseline original
- o tempo do conector caiu de `254.99s` para `236.81s`
- o custo foi um aumento muito forte de memoria, de ~`766 MiB` para ~`3.0 GiB`

Conclusao:

- a nova arquitetura trouxe ganho real de throughput
- o proximo gargalo mais claro passou a ser memoria, e nao somente serializacao serial

### 3. Refatoracao para landing raw com `payload_raw BINARY`

Depois da mudanca do contrato da landing para:

- `payload_raw` como `BINARY`
- `key_raw` como `BINARY`
- remocao de `json.Valid`
- remocao da conversao obrigatoria para `string`

foi executada uma nova matriz comparavel com a mesma carga de `500.000` eventos.

Objetivo:

- medir o ganho do modo binario frente ao modelo textual anterior

## Matriz Comparavel por Codec

Para comparar codecs em condicoes controladas e evitar a saturacao de disco do host, foi rodada uma matriz com:

- `500.000` eventos
- `8 KiB`
- `6` particoes
- mesma configuracao de batch
- mesmo pipeline concorrente novo

### Resultado Consolidado

| Codec | Tempo produtor | Tempo conector | Throughput conector | Pico RSS conector | Volume final | Arquivos |
| --- | --- | --- | --- | --- | --- | --- |
| `zstd` | `118.82s` | `181.71s` | ~`2752 ev/s` | `3019688 KB` | `3687490021` bytes | `54` |
| `snappy` | `138.94s` | `179.01s` | ~`2793 ev/s` | `2733800 KB` | `4023632015` bytes | `54` |
| `uncompressed` | `132.53s` | `124.01s` | ~`4032 ev/s` | `2726632 KB` | `4171246235` bytes | `54` |

### Leitura Rapida da Matriz

- `uncompressed` foi o melhor codec para throughput por ampla margem
- `snappy` foi ligeiramente melhor que `zstd` em tempo total e tambem usou menos memoria
- `zstd` foi o melhor em tamanho final, mas o pior custo-beneficio para esse workload especifico

### Diferenca de Volume Final

Para esse payload de baixa compressibilidade:

- `zstd` gerou cerca de `3.43 GiB`
- `snappy` gerou cerca de `3.75 GiB`
- `uncompressed` gerou cerca de `3.88 GiB`

Ou seja:

- `zstd` economizou espaco
- mas o ganho de tamanho sobre `snappy` e `uncompressed` foi pequeno diante do custo de CPU e do impacto no tempo total

## Matriz Comparavel com Landing Raw Binary

Depois da refatoracao para `payload_raw BINARY`, foi rodada nova matriz em ambiente limpo com a mesma carga:

- `500.000` eventos
- `8 KiB`
- `6` particoes
- mesmo lote e mesma concorrencia

### Resultado Consolidado

| Codec | Tempo produtor | Tempo conector | Throughput conector | Pico RSS conector | Volume final | Arquivos |
| --- | --- | --- | --- | --- | --- | --- |
| `zstd` | `82.09s` | `142.34s` | ~`3513 ev/s` | `2896028 KB` | `3685759371` bytes | `54` |
| `snappy` | `75.07s` | `139.60s` | ~`3582 ev/s` | `3242752 KB` | `4021116995` bytes | `54` |
| `uncompressed` | `143.51s` | `182.92s` | ~`2733 ev/s` | `3318496 KB` | `4170892271` bytes | `54` |

Observacao importante:

- a primeira tentativa de `snappy` binario terminou com `lag` residual e por isso foi descartada
- o resultado valido de `snappy` e o rerun com `TOTAL-LAG 0`

### Comparacao Direta: Textual vs Binary

#### `zstd`

- tempo do conector: `181.71s -> 142.34s` (`-21.67%`)
- RSS: `3019688 KB -> 2896028 KB` (`-4.10%`)
- volume final: praticamente estavel (`-0.05%`)

#### `snappy`

- tempo do conector: `179.01s -> 139.60s` (`-22.02%`)
- RSS: `2733800 KB -> 3242752 KB` (`+18.62%`)
- volume final: praticamente estavel (`-0.06%`)

#### `uncompressed`

- tempo do conector: `124.01s -> 182.92s` (`+47.50%`)
- RSS: `2726632 KB -> 3318496 KB` (`+21.71%`)
- volume final: praticamente estavel (`-0.01%`)

### Leitura Rapida da Mudanca para Binary

- `payload_raw BINARY` ajudou claramente quando havia compressao (`zstd` e `snappy`)
- o ganho principal apareceu em CPU e tempo total, nao em tamanho final
- a mudanca piorou fortemente o perfil de `uncompressed`

Inferencia pratica:

- remover validacao JSON e conversao para string economiza trabalho no caminho quente
- isso beneficia mais os codecs comprimidos
- no caso de `uncompressed`, o writer Parquet com coluna binaria parece ter ficado menos eficiente do que a coluna textual anterior

Conclusao provisoria:

- a direcao `raw binary` faz sentido para o conector
- mas o codec operacional recomendado deixa de ser `uncompressed`
- com o contrato binario, `zstd` e `snappy` passam a ser os perfis mais interessantes

## Falhas e Limites Observados

### Saturacao de disco do host

Durante a bateria maior de `1.000.000` eventos por codec:

- o cenario `snappy` nao terminou limpo
- a causa observada foi o host local ficar sem espaco em disco
- isso interrompeu a execucao antes do fechamento completo do benchmark

Isso nao foi tratado como falha funcional do conector.

### Um rerun de `snappy` binario foi necessario

Na primeira execucao do cenario `snappy` binario:

- o conector encerrou com `TOTAL-LAG 103413`
- o resultado foi descartado da comparacao final

Foi executado novo teste do mesmo cenario e o rerun fechou com:

- `TOTAL-LAG 0`
- `54` arquivos
- `4021116995` bytes finais

Portanto:

- o numero oficial considerado e o rerun
- a primeira execucao foi tratada como anomalia de benchmark

## Atualizacao 2026-04-27: Streaming MinIO com multipart ajustado

### Alteracao aplicada

Para estabilizar o caminho oficial `avro + compression: null + streaming direto`, foi ajustado o upload multipart do MinIO:

- novo parametro `minio.multipart_part_size_mib` com default `16`
- uso explicito de `PartSize` no `PutObject` do MinIO
- uso de `NumThreads: 1` no upload streaming para reduzir pressao de memoria

Arquivos principais:

- `internal/adapters/sink/minio/sink.go`
- `internal/config/config.go`
- `internal/config/config_test.go`
- `configs/orders.minio.example.yaml`

### Motivo tecnico

Sem controle do tamanho de parte no multipart streaming, o caminho de upload pode aumentar buffering interno e causar backpressure excessivo no host local (8 GB RAM).  
Com parte fixa de `16 MiB`, o uso de memoria fica mais previsivel e o fluxo `encode -> upload -> commit` estabiliza para cargas maiores.

### Ganho observado (comparacao direta)

Comparacao entre:

- referencia anterior: `snappy + 2:2:2`
- nova configuracao: `null + streaming + multipart_part_size_mib=16 + 2:2:2`
- mesma carga: payload pseudoaleatorio de `8 KiB`, `6` particoes, `include_headers=true`, `include_key=true`

| Carga | Antes (snappy) | Depois (null+streaming part16) | Delta |
| --- | --- | --- | --- |
| `100k` | `39.03s`, `2562 rec/s`, RSS `597260 KB` | `46.55s`, `2148 rec/s`, RSS `690752 KB` | tempo `+19.3%` (pior), throughput `-16.2%`, RSS `+15.7%` |
| `300k` | `171.46s`, `1749 rec/s`, RSS `603704 KB` | `119.89s`, `2502 rec/s`, RSS `748244 KB` | tempo `-30.1%`, throughput `+43.0%`, RSS `+23.9%` |
| `500k` | `335.47s`, `1490 rec/s`, RSS `623144 KB` | `206.20s`, `2425 rec/s`, RSS `800452 KB` | tempo `-38.5%`, throughput `+62.7%`, RSS `+28.4%` |

### Leitura objetiva do resultado

- para cargas longas (`300k` e `500k`), o novo caminho trouxe ganho forte de tempo e throughput, e completou com `lag=0`
- para carga curta (`100k`), houve regressao de tempo, mas este cenario nao representa o alvo operacional principal
- no `500k`, o tamanho final entre `snappy` e `null` ficou praticamente igual (diferenca de ~`0.07%`), reforcando que o `snappy` nao estava trazendo ganho real para payload pseudoaleatorio neste workload

Conclusao desta alteracao:

- o ajuste de multipart foi decisivo para estabilidade do `500k` em streaming
- a combinacao `compression: null + streaming + multipart_part_size_mib=16 + 2:2:2` passa a ser o perfil recomendado para este ambiente local

### Pressao de memoria continua alta

Mesmo depois das melhorias:

- o novo pipeline continua usando muita memoria
- o teste `1M + zstd` passou de `3 GiB` de RSS
- mesmo `uncompressed`, que foi o melhor em throughput na matriz comparavel, ainda ficou perto de `2.7 GiB`

Isso sugere que ainda existe bastante custo em:

- montagem do lote em memoria
- representacao do payload como `string`
- duplicacoes e alocacoes no caminho de serializacao

## Aprendizados Consolidados

### 1. O pipeline novo melhora throughput, mas desloca o gargalo para memoria

O ganho de throughput veio com:

- workers por particao
- flush concorrente controlado
- upload com tamanho conhecido

Mas o RSS ficou alto demais para um conector que ainda trabalha com lotes de aproximadamente `82 MB` brutos.

Leitura pratica:

- a direcao arquitetural foi correta
- o proximo foco nao deve ser voltar ao pipeline serial
- o proximo foco deve ser reduzir alocacoes e copies no caminho quente

### 2. Para payload pouco compressivel, `zstd` nao parece ser o default ideal

Nos testes atuais:

- `zstd` ganhou em tamanho final
- `snappy` ganhou um pouco em velocidade
- `uncompressed` ganhou muito em throughput

Leitura pratica:

- se a prioridade for throughput, `uncompressed` e hoje a melhor opcao observada
- se a prioridade for equilibrio entre tamanho e desempenho, `snappy` parece o candidato mais forte
- `zstd` so faz mais sentido quando reducao de volume for mais importante que tempo de drenagem

Depois da mudanca para `payload_raw BINARY`, essa leitura muda:

- `uncompressed` deixou de ser a melhor opcao
- `snappy` e `zstd` passaram a ganhar com clareza do modelo binario

Leitura pratica atualizada:

- para landing binaria, `snappy` parece o melhor equilibrio
- `zstd` segue forte quando volume armazenado importa
- `uncompressed` nao deve ser o default do perfil binario sem nova investigacao

### 3. O ambiente local influencia muito a leitura dos resultados

Dois efeitos ficaram claros:

- CPU e memoria do Redpanda alteram fortemente o perfil do teste
- espaco em disco do host pode invalidar execucoes longas mesmo quando o conector esta correto

Leitura pratica:

- comparacoes entre execucoes precisam sempre registrar a capacidade do ambiente
- para baterias maiores, vale preparar um profile dedicado com mais disco livre

### 4. O batch continua sendo limitado por `max_records`

Nos logs de flush, o comportamento dominante permaneceu:

- `records: 10000`
- `bytes_approx` por lote na faixa de `82 MB`

Leitura pratica:

- o tuning principal de tamanho de lote continua comecando por `max_records`
- aumentar apenas `max_bytes` dificilmente mudara esse perfil

## Recomendacao Atual

Se o objetivo principal for drenagem rapida de backlog em workload parecido com este:

- recomendacao inicial para o contrato binario: `snappy`

Se o objetivo for equilibrio entre espaco e desempenho:

- recomendacao inicial: `snappy`

Se o objetivo for minimizar volume armazenado:

- recomendacao inicial: `zstd`, aceitando maior custo de CPU e tempo

## Proximos Passos Recomendados

### Prioridade 1. Atacar memoria e copies

Acoes:

- medir heap e allocs com `pprof` nos tres codecs
- revisar `internal/batch/assembler.go` para reduzir copies de payload, key e headers
- reavaliar `payload_json` como `string` no caminho quente
- estudar representacao binaria ou conversao JSON opcional

### Prioridade 2. Revisar estrategia de lote

Acoes:

- testar `max_records` menor, como `5000`, para ver se o tradeoff de throughput vs RSS melhora
- comparar variacao de `max_parallel_flushes`
- observar se menos concorrencia reduz RSS sem derrubar muito o throughput

### Prioridade 3. Padronizar perfis operacionais

Criar perfis explicitos:

- perfil `throughput-first`: `uncompressed`
- perfil `balanced`: `snappy`
- perfil `storage-first`: `zstd`

### Prioridade 4. So depois avaliar mudanca mais profunda de stack

Apache Arrow ou mudanca maior de stack ainda nao sao a proxima etapa natural.

Primeiro precisamos confirmar com `pprof`:

- quanto da CPU vai para compressao
- quanto vai para serializacao Parquet
- quanto vai para alocacao e GC

Sem essa confirmacao, trocar de stack agora aumentaria complexidade antes de resolver o gargalo mais evidente.
