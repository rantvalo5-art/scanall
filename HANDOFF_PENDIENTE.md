# HANDOFF — todo lo que queda por hacer

> Escrito el **2026-08-26**. Este es el **único punto de entrada**: los handoffs anteriores
> (`HANDOFF_UNLOCKS.md`, `HANDOFF_SENALES.md`, `HANDOFF_BASIS.md`, `HANDOFF_CIERRE.md`)
> quedan como registro histórico de lo cerrado, pero su sección "qué sigue" está vencida.
> ~~Empezar por la **sección 1**, que es lo único que bloquea.~~ **La sección 1 quedó
> RESUELTA el 2026-08-26** (ver el bloque verde ahí). Ya no bloquea nada: lo que sigue son
> las decisiones abiertas de la **sección 6** y las familias sin tocar de la **sección 4**.
>
> Nada de esto está commiteado. Rama actual: **`banco/primer-toque`**.

---

## 0. Cómo arrancar (verificado, no de memoria)

Hay **dos benches distintos** en el repo y se corren distinto:

```bash
# BANCO DE HIPOTESIS — se corre DESDE banco/
cd banco
py -3.13 -u lote.py                        # batería estándar de precio
py -3.13 -u micro.py --nula 5              # calibrar look-elsewhere de un lote
py -3.13 -u test_unlocks.py --tendencia    # test de evento esparcido

# DAY TRADER (raíz) — se corre desde la raíz
py -3.13 -u backtest.py --weeks 1 --max-pairs 200 --scan-interval-min 2
```

**Gotchas operativos, todos pisados en carne propia:**

- Siempre `py -3.13`, nunca `python` (el del sistema no tiene las deps).
- Siempre `-u`. Sin eso una corrida de horas no muestra nada y si muere el log queda vacío.
- `$env:PYTHONIOENCODING = "utf-8"` antes de correr, o revienta con cp1252.
- El writer de parquet falla en esta máquina: el caché del banco es `.csv` por fallback.
- Los procesos se llaman `python3.13`, **no** `python.exe`.
- Corridas pesadas **de a una**, nunca en paralelo.
- **`ta` calcula Bollinger con `ddof=0`**, no con el default de pandas (`ddof=1`). Si
  reimplementás cualquier indicador de `ta`, verificalo numéricamente contra `ta` antes de
  confiar: la diferencia es un sesgo constante de 2,6% en una sola dirección y **no falla
  nada**, sólo desalinea.
- **Nunca `groupby.apply` con lambdas** sobre miles de grupos por par en el banco: en
  `micro.py` costaba 273s por par y vectorizado quedó en 0,10s (2700×).

**Cachés (grandes, no borrar sin querer):**

| caché | qué tiene | tamaño |
|---|---|---|
| `banco/.kline_cache/` | 1h de varios universos + **5m de base200 2025-08→2026-08 (200 pares)** | 1,7 GB |
| `.backtest_cache/` | klines del day trader + **1m de 200 pares: 2026-06-20→2026-08-15 y 2026-08-19→2026-08-26** | 1,77 GB en 1m |
| `banco/.unlock_cache/` | calendario DefiLlama ya extraído | KB |
| `fade/.cache/dt_all.json` | alertas en vivo del day trader | — |

---

## 1. ~~LO ÚNICO PENDIENTE~~ → **RESUELTO el 2026-08-26**

> ### ✅ ADJUDICADO: el cableado SÍ cierra la brecha de composición
>
> A/B **pareado**, 56 días (2026-06-20 → 2026-08-15), 200 pares, `--scan-interval-min 2`,
> partido en **4 trozos de 2 semanas por lado** (8 corridas, ~45 min c/u, todas OK).
>
> | | OFF (`--no-forming-1m`) | ON | vivo |
> |---|---|---|---|
> | EXPLOSION/BREAKOUT | **0,792** (267/337) | **1,343** (458/341) | 1,39 |
> | alertas/día | 22,2 | **25,6** | 26,2 |
>
> **Regla de parada de la 1.5, escrita antes de mirar:** ≥ 1,25 ⇒ explica la brecha.
> Dio **1,343**. Multiplicador entregado **×1,70** (hacía falta ×1,75 para igualar al vivo).
>
> **Sin desplazamiento entre señales** (era lo que podía dar vuelta la lectura): no-EXPLOSION
> agregado **978 → 976** (−2 alertas, −0,20%) contra ruido Poisson ±31, y **0 casos** de
> mismo `(symbol, ts)` cambiando de tipo. PREBREAK rompió el ±2% de la 1.4 (−3 sobre base 77)
> pero es **sub-ruido** (Poisson ±8,8): el ±2% por señal es demasiado fino para base chica.
>
> **Dos correcciones al plan de abajo, ambas medidas:**
>
> 1. **La 1.4 estaba mal: `puente2.json` NO sirve como lado OFF.** Su ventana es
>    2026-06-21 → **08-16**, corrida **un día**, y su top-200 se rankeó el 25-ago (no el 26);
>    ninguna de las dos corridas tenía `SUPABASE_KEY`. Contra la misma ventana daba
>    BREAKOUT +27% / HOLD +21%, imposible para un detector que sólo agrega EXPLOSION.
>    `config.json` NO cambió (intacto desde 28-may). Por eso se corrió **el OFF de verdad**.
> 2. **La 1.3-ter no era gratis: el caché indexa por `(start_ms, end_ms)` EXACTOS**, así que
>    cada trozo cambiaba la clave y **redescargaba los 200 pares × 1m** — justo el costo que
>    la 1.2 daba por pagado. Se agregó **`_cache_slice()`** en `backtest.py`: ante un miss
>    recorta un superconjunto ya cacheado. Validado con **0 mismatches interiores en 120
>    pares con contención** (lo único que difiere es la última vela forming de los archivos
>    viejos, donde el recorte es el dato *más* correcto). El trozo 1 bajó **29 archivos en
>    vez de ~1000**.
>
> **Por qué moría la monolítica y los trozos no:** trozos ~2,0M tareas / ~45 min; la de 8
> semanas eran **8,0M** y ~2,9 h **sin imprimir una línea** (`Parallel(loky)` es mudo entre
> el dispatch y el resultado). Medido durante un trozo: padre 1,63 GB + 12 workers ×0,08 GB
> = **~2,6 GB** contra 11,8 GB libres → **memoria no era**, como ya decía la 1.3-bis.
>
> **Salidas:** `bt_p2_on_<END>.json` y `bt_p2_off_<END>.json` (4 + 4).
> **Sigue en pie la 1.6:** esto NO toca la saturación por cadencia.

<details><summary>Plan original (histórico, ya ejecutado)</summary>

### 1.0 relanzar la corrida de 8 semanas

### 1.1 Qué es y por qué importa

El ítem 4.1 (cablear `EXPLOSION_FORMING` al loop del backtest) **está hecho y validado
cuatro veces** (ver sección 2.3). Lo que falta es **una sola medición**: si el cableado
cierra o no la brecha replay-vivo que diagnosticó `fade/PUENTE2.md`.

La corrida se lanzó el 2026-08-26 y **se detuvo a poco de arrancar** — el log quedó en 16
bytes, sin resultados. No se relanzó.

### 1.2 La buena noticia: la parte cara ya está pagada

La descarga de 1m —que es el 80% del costo— **ya se completó antes de que la mataran**:

```
.backtest_cache/  →  200 pares × 1m × 2026-06-20 → 2026-08-15 (56 días)
```

Relanzar **no vuelve a descargar nada**. Sólo cuesta simulación.

**Estimación real, medida:** la corrida de 1 semana sin descarga tardó **1.321 s**. Ocho
semanas son 8× los scans (5.040 → 40.320), o sea **~2,9 h por lado**.

### 1.3 ESTADO: relanzada 3 veces el 2026-08-26, murió las 3

**No hay resultado todavía.** Se relanzó tres veces y murió siempre en el mismo punto
(sección 1.3-bis). Se dejó de insistir a propósito: tres fallas idénticas son un patrón,
no mala suerte, y una cuarta corrida igual habría dado lo mismo.

**El próximo paso NO es relanzar el comando entero, es la sección 1.3-ter** (partir la
ventana en cuatro trozos de 2 semanas). Si `bt_p2_on*.json` existe al abrir esto, ya se
corrió: ir al chequeo de la 1.4 y a la adjudicación de la 1.5.

### 1.3-bis Murió TRES veces en el mismo punto — diagnóstico

Las tres veces murió **en el dispatch paralelo**, después de bajar todo y precomputar.
Última línea del log, idéntica siempre:

```
[analyze] 40321 scans × ~200 pares = 8064200 tareas (fusionado)
[killed]
```

**Hipótesis 1 (memoria) — PROBADA Y DESCARTADA.** Se sospechó que el 1m reventaba la RAM:
el df crudo trae 12 columnas y dos (`quote_vol`, `ignore`) son dtype **object**, o sea
strings de Python — 14,0 MB por par, **2,80 GB** para 200 pares. Se recortó a las 6
columnas que `_build_partial_bar` realmente lee → 3,5 MB/par, **0,70 GB**, un 75% menos.

**La corrida volvió a morir igual.** Y midiendo la huella real del proceso padre:

| componente | GB |
|---|---|
| klines 5m/15m/1h/4h | 0,77 |
| klines 1m (recortado) | 0,70 |
| `prepared` (precompute, ~5,8 MB/par sólo en 5m) | ~1,73 |
| `bar_idx_cache` | 0,26 |
| **total padre** | **~3,46** |

Con **11,8 GB disponibles** (25,4 totales, 12 cores). **No es memoria del padre.**
El recorte de columnas se dejó igual porque es higiene buena, pero **no era la causa**.

**Hipótesis 2 (límite de duración del proceso en background) — la que queda en pie.**
El paso `Parallel(n_jobs=-1, backend="loky")` **no imprime nada** hasta terminar, y a 8
semanas eso son ~2,9 h de silencio. Las tres muertes ocurren exactamente al entrar en ese
silencio. La corrida de 1 semana —que sí terminó— tardó 1.321 s y nunca estuvo tanto
tiempo sin escribir. Encaja con las tres muertes y explica por qué el arreglo de memoria
no cambió nada. **No está confirmado**: para confirmarlo hay que correrlo en primer plano
o instrumentar el dispatch.

### 1.3-ter LA SALIDA: partir la ventana en trozos

Independientemente de cuál sea la causa, **la ventana partida la esquiva a las dos**: menos
memoria por corrida y, sobre todo, cada trozo termina en ~45 min en vez de estar 2,9 h
mudo.

```bash
# cuatro trozos de 2 semanas; cada uno pesa como la corrida de 1 semana que SI funciono
for END in 2026-07-04 2026-07-18 2026-08-01 2026-08-15; do
  py -3.13 -u backtest.py --weeks 2 --end-date $END --max-pairs 200       --scan-interval-min 2 --out bt_p2_on_$END.json
done
```

Después se concatenan los `["main"]` de los cuatro JSON y se cuentan los `signal_type`
como en la sección 1.5.

**Dos cosas que NO hay que hacer:**

- **No bajar `--max-pairs`.** Cambiaría el universo y rompería la comparación contra
  `puente2.json`, que es todo el punto del ejercicio.
- **No subir `--scan-interval-min`.** Con múltiplo de 5 el detector no dispara nunca
  (sección 1.3).

**Ojo con el solapamiento:** trozos de 2 semanas terminando en esas 4 fechas cubren
2026-06-20 → 2026-08-15 sin huecos ni repetición. Verificar igual que la suma de alertas
no tenga `(symbol, alerted_at)` duplicados antes de contar.

### 1.3-quater El comando entero (los dos lados, si hiciera falta el OFF)

```bash
cd "C:/Users/asd/Saved Games/scancrypto/scanall"
$env:PYTHONIOENCODING = "utf-8"

# lado ON (detector cableado)
py -3.13 -u backtest.py --weeks 8 --end-date 2026-08-15 --max-pairs 200 \
    --scan-interval-min 2 --out bt_p2_on.json

# lado OFF (comportamiento previo) — ver 1.4: quizá no haga falta
py -3.13 -u backtest.py --weeks 8 --end-date 2026-08-15 --max-pairs 200 \
    --scan-interval-min 2 --no-forming-1m --out bt_p2_off.json
```

⚠️ **`--scan-interval-min 2` no es opcional.** Con un intervalo múltiplo de 5 todos los
scans caen en el borde de la vela de 5m, **el detector forming no dispara nunca**, y la
corrida diría "no cambia nada" — conclusión falsa. Hay un aviso en `main()` que lo grita,
pero conviene saberlo antes. Producción corre cada 2,00 min.

### 1.4 Cómo ahorrarse la mitad (recomendado)

**`fade/puente2.json` YA ES el lado OFF de esa ventana.** Se generó con el código previo,
misma ventana (2026-06-21 → 2026-08-15), `--scan-interval-min 2`, 200 pares. Y el test de
regresión probó que el código nuevo con `--no-forming-1m` produce alertas **idénticas** a
`git HEAD` (94 vs 94, byte a byte).

O sea: **correr sólo el lado ON** y comparar contra `puente2.json` ahorra ~3 h.

**El chequeo que valida ese atajo, y que hay que hacer sí o sí:** el cableado sólo agrega
alertas EXPLOSION; BREAKOUT / HOLD / PREBREAK / RIDING tienen que quedar **casi idénticos**.
`puente2.json` tiene `BREAKOUT=280, HOLD=259, RIDING=207, PREBREAK=69`. Si el ON da esos
números (±2%), `puente2.json` es un OFF legítimo y la comparación vale. Si difieren mucho,
el config cambió entre medio y hay que correr el OFF de verdad.

### 1.5 Cómo se adjudica — y la trampa de denominador

Números de referencia, ya recalculados de los archivos (no de memoria):

| fuente | ventana | EXPLOSION/BREAKOUT | alertas/día |
|---|---|---|---|
| **en vivo** (`PUENTE2.md`) | 51 días medidos | **1,39** (792/571) | 26,2 |
| replay OFF (`puente2.json`) | 56 días | **0,84** (234/280) | 18,7 |
| replay OFF (1 semana, ago-2026) | 7 días | 0,35 (51/146) | 72,9 |
| replay ON (1 semana, ago-2026) | 7 días | **0,64** (94/146) | 78,4 |

> ⚠️ **NO comparar el 0,64 de la corrida de 1 semana contra el 1,39 del vivo.** Son
> ventanas distintas y la composición depende fuerte del período — mirá cómo el mismo
> replay OFF da 0,84 en 56 días y 0,35 en una semana. Ese error casi se comete: iba a
> reportarse "cierra el 28% de la brecha", que es un número mal construido.

**Entre ventanas distintas lo único comparable son multiplicadores:**

- lo que el cableado entrega (medido, 1 semana): **×1,84** sobre el ratio;
- lo que haría falta para igualar al vivo: 0,84 → 1,39 = **×1,66**.

Del orden correcto, **pero no demostrado**. La corrida de 8 semanas cierra eso porque pone
ON y el vivo en el mismo denominador.

**Regla de parada, escribila antes de mirar:** el cableado explica la brecha de composición
si el ratio ON de la ventana de 56 días llega a **≥ 1,25** (90% del 1,39 del vivo). Si
queda en 1,0–1,25, explica parte y **falta otra causa**. Si queda ≤ 1,0, el forming no era
la causa principal y hay que volver a `PUENTE2.md` a buscar.

### 1.6 Lo que esta corrida NO va a arreglar, pase lo que pase

La otra mitad del diagnóstico sigue en pie: **el replay sólo ve velas cerradas**. Dos scans
dentro de la misma vela de 5m ven datos idénticos para todo lo que no sea EXPLOSION
forming. La saturación por cadencia (2 min ≈ 5 min en alertas/día) **no se toca con esto**.
El cableado agrega alertas que antes no existían; no agrega resolución temporal al resto.

---

</details>

---

## 2. En qué estado quedó cada cosa (para no re-hacerla)

### 2.1 Unlocks — CERRADO

`banco/PREREGISTRO_UNLOCKS_2.md`. Test de tendencia continua, two-sided por contaminación
declarada. **β = −5,91 pp de win rate por década de dosis** (n=1.036, 78 símbolos).
4 de 6 compuertas pasan; fallan las dos de significancia (p permutación 0,1093; IC95 por
bootstrap de símbolos [−12,76 , +0,83]).

Se cierra porque el efecto es **más chico que lo que 1.040 eventos distinguen de cero**, y
1.040 es el techo de la data (478 pre-listado, 913 futuros). **No es "subpotenciado"**: el
MDE se fijó en 6,6 pp/década *antes* de estimar.

**Lo que hay que recordar:** el prior contaminado apuntaba al revés. La corrida 1 (buckets)
insinuaba +5,3 pp *contra* H; la tendencia dio negativo, la dirección de H. No es
contradicción: los buckets comparan *entre* monedas y la pendiente se identifica **51%
dentro de cada moneda**. **Escribirlo two-sided fue lo que salvó la corrida.**

### 2.2 Microestructura intra-vela — CERRADA

`banco/PREREGISTRO_MICRO.md`, `banco/micro.py`. 12 features imposibles de calcular con
velas de 1h (forma del camino intra-hora + tres de liquidez: Amihud a 5m, autocorrelación
lag-1, spread de Roll), ×2 suavizados ×2 colas ×2 versiones ×2 direcciones = **192
hipótesis** sobre 111.330 entradas de 187 pares.

**0 sobrevivientes.** Pero **65 cruzan el umbral y las 65 mueren en el mismo lugar**: la
corrección por multiplicidad, porque el p por bloques dice que no hay nada.

> **37 hipótesis con p_indep < 0,001. CERO con p_bloques < 0,05.**
> La mejor: **p_indep 1,1e-36 y p_bloques 0,3845.**

Es el caso más extremo que dio el repo de esa brecha. Con 22.000 entradas solapadas y
régimen autocorrelacionado, el n efectivo no es 22.000: es ~52 (las semanas).

**Lo que SÍ quedó demostrado:** la forma del camino **no es volatilidad disfrazada** — de
las 19 crudas con margen >+1 pp, la versión condicionada al quintil de `atr_24` retiene el
**94%** del margen. Es información distinta; le falta consistencia temporal.

**La historia coherente que no sirve, anotada para no redescubrirla:** las 8 mejores son
todas del lado corto — `amihud bajo`, `tk_sd bajo`, `hhi bajo` → **las horas ordenadas y
líquidas preceden caídas**. Sobrevive el control pareado (+3,25 pp). Vive en pocas semanas.

### 2.3 Detector forming cableado — HECHO y **MEDIDO** (sección 1, 2026-08-26)

`backtest.py`: `build_forming_features()` + bloque "EXPLOSION sobre vela FORMING" en
`classify()`, espejo del de `screener.py`. El 1m viaja dentro de `klines[sym]["1m"]` →
**ninguna firma cambió**. `--no-forming-1m` lo apaga.

**Cuatro validaciones:**

| test | resultado |
|---|---|
| espejo del screener, 305 instantes reales | idéntico salvo 1,4e-11 (ruido de float en BB) |
| **lookahead**: se corrompe TODA la data posterior a `ts` | **243/243 salidas idénticas** |
| regresión con el detector apagado vs `git HEAD` | **94 vs 94 alertas, idénticas** |
| A/B 200 pares / 1 semana | sólo cambia EXPLOSION; el resto intacto |

**Tres bugs silenciosos encontrados y arreglados** (los tres habrían dado conclusiones
falsas sin que nada fallara): el `ddof` de `ta`; el intervalo de scan múltiplo de 5;
y `_analyze_key()` sin `EXPLOSION_FORMING`, que hacía que un `--compare` entre configs con
y sin forming reutilizara los mismos candidatos.

### 2.4 Lo que NO hay que volver a proponer

Precio en todas sus transformaciones (450+ hipótesis) · régimen (7 detectores × 22
trimestres) · salida/timing (7 confirmaciones: mueven mediana y cola, nunca la media) ·
straddles (sin convexidad con órdenes stop) · ML sobre las 36 features viejas (techo
condicional medido) · volumen y forma de vela a resolución horaria (0/86) ·
**microestructura intra-vela en el top-200** (0/192, esta sesión) · **unlocks** (cerrado
por límite de datos).

---

## 3. Las reglas de método que rigen

Heredadas y confirmadas. Estas no se negocian:

> **La regla de parada se escribe antes de mirar.** Si se afloja después de ver un número,
> el experimento no vale.

> **El p que decide es el de bloques**, no el binomial. La brecha entre los dos *es* el
> autoengaño. Caso extremo medido: p_indep 1,1e-36 → p_bloques 0,3845.

> **`sin_top3` antes que cualquier otra cosa.** En unlocks, las cuatro hipótesis con efecto
> visible dieron vuelta el signo al sacar 3 símbolos.

> **En un estudio de evento, contar el n POST-JOIN antes de escribir la regla de parada.**
> El censo de unlocks decía 2.707; el bucket decisivo quedó en n=143.

> **Calcular el MDE con la nula real ANTES de estimar el efecto.** Permutar da sd(β) sin
> revelar el resultado, y convierte un "no se pudo medir" en "no está".

> **La nula de look-elsewhere se hace por DESPLAZAMIENTO CIRCULAR, no permutando filas.**
> Barajar destruye la autocorrelación de features y resultados y hace la nula demasiado
> fácil. Implementado en `micro.py --nula N`.

> **Si el test es contra un prior contaminado, escribirlo TWO-SIDED.**

> **Ojo con el denominador al comparar corridas.** La composición depende del período: el
> mismo replay da ratio 0,84 en 56 días y 0,35 en 7. Entre ventanas distintas sólo son
> comparables multiplicadores.

---

## 4. Lo que queda sin explorar

> **4.1 y 4.3 quedaron CERRADOS el 2026-08-26** (ver abajo). Queda solo **4.2**
> (eventos de listado), que el propio handoff marca como la trampa de unlocks: n chico.

Ninguna comparte causa de muerte con lo cerrado. En orden de lo que yo haría:

### 4.1 ~~Lead-lag entre monedas~~ — **CERRADO el 2026-08-26**

> **0 de 384**, con la barra de la nula circular en >= 1. Ver `banco/PREREGISTRO_LEADLAG.md`,
> `banco/leadlag.py`, `banco/leadlag.csv` (rama `banco/primer-toque`, commit a21740c).
> **Subpotenciadas: 0** — fueron juzgadas, no omitidas.
>
> Se esquivo la matriz 200x200 midiendo lead-lag **por grupo** (Lo-MacKinlay): 4
> caracteristicas x 4 lags x 3 grupos = 48 features -> 384 hipotesis, con leave-one-out
> obligatorio. Sin lookahead (48/48 features identicas corrompiendo toda la data posterior).
>
> **Lo que hace fuerte al negativo:** las 149 que cruzan el umbral son **todas del lado
> corto** (149 a 0) y estan repartidas **uniformemente** entre las 4 caracteristicas
> (41/38/36/34) y los 4 lags (34/38/40/37). Si el desfase fuera real se concentraria en
> celdas concretas; que aparezca por igual en todas dice que no depende de la feature.
> Es **deriva del periodo**: el largo base (48,65%) esta 3,35 pp debajo del "sin deriva"
> (52,00% — las barreras +-8% son asimetricas en log), asi que el corto arranca en
> +0,10 pp sobre el break-even y el largo en -2,60 pp.
>
> Y otra vez la firma de siempre: **110 con p_indep < 0,001, solo 12 con p_bloques < 0,05**
> (la mejor: 2,3e-44 contra 0,1735). Igual que microestructura.
>
> **Queda sin descartar:** lead-lag mas fino que 1h, por pares especificos, y fuera del
> top-200.

<details><summary>Planteo original</summary>


¿Unas se mueven sistemáticamente antes que otras? **Todo lo probado en este repo es serie
de tiempo por símbolo o transversal contemporáneo** — nunca se miró el desfase.

- **A favor:** es información genuinamente nueva, y el panel horario de `base200` ya está
  cacheado (no cuesta datos).
- **En contra:** el candidato obvio (BTC lidera a las alts) ya está medido indirectamente —
  `beta_btc` e `idio_168` murieron en `lote_ancho.py`. Lo que queda sin mirar es lead-lag
  **entre alts**, que es una matriz de 200×200 lags: eso es look-elsewhere puro y necesita
  la nula por desplazamiento circular de entrada, no como chequeo posterior.
- **Costo:** bajo en datos, medio en diseño. La trampa es la multiplicidad.

</details>

### 4.2 Eventos de listado en Binance

Anuncios de listado: timestamp exacto, efecto documentado en la literatura, y **no sale del
precio**.

- **A favor:** es la única familia de eventos que queda con mecanismo causal claro. Y la
  maquinaria de estudio de evento esparcido **ya está construida y validada**:
  `preparar()` + `_p_permutacion()` + `_ic_bootstrap()` en `banco/test_unlocks.py` sirven
  tal cual.
- **En contra:** hay que conseguir la data (no hay endpoint; se scrapea el blog de Binance
  o se usa un dataset de terceros), y **el n va a ser chico** — pocos listados por mes. Es
  exactamente la trampa de unlocks, así que **contar el n post-join y el MDE antes de
  escribir la regla** no es opcional acá.

### 4.3 ~~La cola ilíquida~~ — **CERRADO el 2026-08-26, por costos**

> No se llego a correr la hipotesis: **la fase 0 la cerro**. El handoff decia que habia
> que rehacer los costos antes de creerle a un solo numero, y rehacerlos alcanzo.
> `banco/libro.py`, `libro.csv`, `libro_10k.csv` (rama `banco/primer-toque`, 74818fc).
>
> **Primero, lo que NO funciona:** estimar el spread de OHLC con Corwin-Schultz o Roll.
> El rango high-low mediano de una hora de BTCUSDT son ~49 bps contra un spread real de
> ~1 bp, asi que CS mide volatilidad: 8,4 bps en 1h, 42,9 en 1d, y **plano** entre
> cuartiles de volumen (0,212/0,240/0,231/0,206%). Y el piso de ruido **escala con la
> volatilidad**, o sea que habria inflado la cola por volatil y no por iliquida —
> el artefacto exacto que 4.3 tenia que evitar, disfrazado de modelo de costos.
>
> **Lo que si:** medir el libro con `/api/v3/depth` y caminarlo hasta llenar la orden.
> Valida donde CS fallaba (rank 1-50 da 1,3 bps, el numero real de BTCUSDT).
>
> | banda | orden $1k | orden $10k | win rate necesario (8%/8%) |
> |---|---|---|---|
> | rank 1-50 | 0,230% | 0,279% | 51,44% / 51,74% |
> | rank 51-200 | 0,339% | 0,597% | 52,12% / 53,73% |
> | rank 201-400 | 0,441% | 0,994% | 52,76% / **56,21%** |
> | rank 401-600 | 0,524% | 1,261% | 53,28% / **57,88%** |
>
> Entre **1,5x y 6,3x** el 0,20% supuesto. **La cola no es terreno mas facil con la misma
> vara: es una vara mas alta.** A tamano operable pide **57,88%**, y el mejor efecto que
> el repo vio alguna vez fue **+4,71 pp in-sample** (lead-lag, muerto en bloques). El
> requisito supera al mejor artefacto, asi que se cierra por la misma logica de MDE que
> cerro unlocks.
>
> **Nada previo se da vuelta:** todas las familias cerradas se cerraron contra un costo
> demasiado barato; subirlo solo las mata mas.

<details><summary>Planteo original</summary>


Todo se midió sobre `base200`, donde la competencia es máxima. Varias features de
microestructura (Amihud, Roll, `ac1_5m`) son **mecánicamente más grandes abajo**.

- **A favor:** es un cambio de *terreno*, no de información — la única de las tres palancas
  que el repo nunca movió. Y `micro.py` corre tal cual sobre otro universo.
- **En contra, y es serio:** **el modelo de costos está mal ahí.** `COSTO_PCT = 0,20%` sin
  slippage es optimista por construcción justo donde el spread es ancho — y las features
  que más prometen son literalmente medidas de iliquidez. **Hay que rehacer los costos
  antes de creerle a un solo número**, y eso es media sesión de infraestructura antes del
  primer resultado. Sin eso, cualquier hallazgo es un artefacto de contabilidad.

</details>

**Ventana OOS virgen: 2024-08-01 → 2025-08-01.** Declarada en `PREREGISTRO_ANCHO.md` y
**nunca mirada**, porque nunca sobrevivió nada que promover. Sigue disponible.

---

## 5. Inventario de esta sesión

**Creados:**

```
banco/PREREGISTRO_UNLOCKS_2.md   preregistro (contaminación declarada) + resultados
banco/unlocks_tendencia.csv|.log los 1.036 eventos y la corrida
banco/PREREGISTRO_MICRO.md       preregistro ciego + resultados
banco/micro.py                   12 features intra-vela + test condicional a volatilidad
banco/micro.csv|.log             las 192 hipótesis
banco/micro_conteo.log           conteo post-join
banco/micro_nula.log             calibración look-elsewhere (0 de 192 en 5 reps)
bt_forming_on.json / _off.json   A/B piloto 25 pares
bt_fx_on.json / bt_fx_off.json   A/B 200 pares, 1 semana
bt_head_ref.json                 salida de git HEAD (test de regresión)
HANDOFF_PENDIENTE.md             este archivo
```

**Modificados:**

- `backtest.py` (+214 líneas) — detector forming en el loop, `_analyze_key` corregido,
  `--no-forming-1m`, aviso de intervalo múltiplo de 5.
- `screener.py` — **sólo un comentario** de sincronía apuntando al espejo en `backtest.py`.
  El resto del diff de ese archivo es trabajo previo a esta sesión (redacción de tokens).
- `banco/klines.py` — `load_panel(..., workers=N)`, descarga paralela por par.
- `banco/test_unlocks.py` — modo `--tendencia`.
- `fade/PUENTE2.md` — adenda con el cableado.
- `HANDOFF_UNLOCKS.md` — secciones 3, 4.1, 6 y 7 actualizadas.

**Memorias:** `project-unlocks-primera-corrida` (reescrita a CERRADO),
`project-microestructura-cerrada` (nueva), `project-replay-brecha-diagnosticada`
(actualizada con el cableado).

---

## 6. Decisiones abiertas (son tuyas, no las tomé)

1. **La rama.** Todo esto está en `banco/primer-toque`. El cableado del forming toca la
   **raíz** (day trader), y la convención del repo dice que el day trader usa `day/*`.
   Habría que decidir si se parte en dos ramas o queda junto.
2. **Commitear.** **No se commiteó nada.** Hay cambios en 4 archivos versionados y ~20
   archivos nuevos sin trackear en `banco/`.
3. **Si vale la pena seguir con el day trader.** Está medido y cerrado (pierde en todo
   horizonte, peor que al azar). El cableado del forming mejora *el instrumento*; no
   cambia ese veredicto. Si la corrida de la sección 1 confirma que cierra la brecha, la
   pregunta siguiente es si se re-mide el day trader con el instrumento arreglado o si se
   lo deja cerrado y el instrumento queda para otra cosa.

---

**Lo que este repo mide, dicho sin vueltas:** el mercado es eficiente respecto de la
información que hay en el precio, en las 200 monedas más líquidas, a horizonte de días a
semanas. Eso no es un fracaso del método — es el resultado correcto para el segmento más
competido. Cambiar la respuesta requiere cambiar **la información** (datos que no salgan
del precio), **el terreno** (la cola ilíquida, con costos rehechos) o **el juego** (dejar
de predecir dirección). Iterar más sobre lo mismo sube la vara del azar, no baja la del
hallazgo.
