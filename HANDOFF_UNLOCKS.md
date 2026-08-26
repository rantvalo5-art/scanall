# HANDOFF — Unlocks, y lo que se cerró el 2026-08-24/25

> Abrir en conversación nueva y empezar por la **sección 6**.
> Escrito el **2026-08-25**. Complementa `HANDOFF_SENALES.md` (presupuesto agotado) y
> `fade/PUENTE.md` + `fade/PUENTE2.md` (la brecha replay-vivo, ya diagnosticada).
>
> Todo el trabajo de esta sesión vive en `banco/` (el banco de hipótesis) y `fade/`.
> **No se tocó `swing/` ni la raíz salvo `backtest.py` en modo lectura.**

---

## 0. Cómo arrancar

```bash
# el banco se corre DESDE banco/
cd banco
py -3.13 -u lote.py                  # batería estándar de precio (30 hipótesis)
py -3.13 -u movers.py --escalera     # quintiles de volatilidad
py -3.13 -u test_unlocks.py          # el hilo abierto
```

**Gotchas operativos, todos verificados esta sesión:**

- Siempre `py -3.13`, nunca `python` (el del sistema no tiene las deps).
- Siempre `-u`. Sin eso la salida queda buffereada y una corrida de horas no muestra nada
  — y si el proceso muere, el log queda vacío.
- `$env:PYTHONIOENCODING = "utf-8"` antes de correr, o revienta con cp1252.
- **El writer de parquet falla en esta máquina**: todo el caché es `.csv` vía fallback.
  No es un bug nuevo, es de antes. Funciona, solo es más lento.
- Los procesos se llaman `python3.13`, **no** `python.exe` — `tasklist` con `python.exe`
  dice que no hay nada corriendo aunque haya 12 workers.
- Corridas pesadas de a una, no en paralelo.
- Cachés: `banco/.kline_cache/` (sufijo `_v2` = velas anchas con volumen),
  `banco/.unlock_cache/`, `fade/.cache/dt_all.json` (alertas en vivo del daytrader).
- La key anon de Supabase está commiteada en `fade/evaluar.py` (~línea 32) si hace falta.

---

## 1. Las reglas de método que rigen

Heredadas y confirmadas esta sesión:

> **La regla de parada se escribe antes de mirar.** Si se afloja después de ver un número,
> el experimento no vale. Esta sesión respetó eso tres veces (`PREREGISTRO_ANCHO.md`,
> `PUENTE2.md`, `PREREGISTRO_UNLOCKS.md`) y una de esas veces impidió una cuarta corrida
> del puente que habría sido pesca.

> **El p que decide es el de bloques**, no el binomial. La brecha entre los dos es
> literalmente el autoengaño. Esta sesión lo volvió a ver: `beta_btc bajo` daba p_indep
> 0,0000 y p_bloques 0,3815.

> **`sin_top3` antes que cualquier otra cosa.** En unlocks, las cuatro hipótesis con efecto
> visible **dieron vuelta el signo** al sacar 3 símbolos.

**Reglas nuevas, que salieron de esta sesión:**

> **Calcular el MDE con la nula real ANTES de estimar el efecto.** Permutar la muestra da
> sd(beta) sin revelar el resultado, y convierte un "no se pudo medir" en "no está". En
> unlocks 2 el MDE quedó fijado en 6,6pp/década antes de mirar, y por eso el cierre es
> adjudicable. En micro la nula dio 0/192, y por eso la regla pudo pedir "al menos uno".

> **La nula de look-elsewhere se hace por DESPLAZAMIENTO CIRCULAR, no permutando filas.**
> Barajar destruye la autocorrelación de features y resultados y hace la nula demasiado
> fácil. Correr las features dentro de cada símbolo conserva ambas estructuras y rompe
> sólo la alineación, que es la hipótesis nula. Implementado en `micro.py --nula N`.

> **Si el test es contra un prior contaminado, escribirlo TWO-SIDED.** En unlocks 2 el
> prior sucio apuntaba a +; el dato dio −. One-sided lo habría leído como refutación.


> **En un estudio de evento, contar el n POST-JOIN antes de escribir la regla de parada.**
> El censo de unlocks decía 2.707 eventos; después de agregar por (símbolo, día), descartar
> los pre-listado y exigir horizonte de futuro quedaron **1.040**, y el bucket que la regla
> usaba para decidir cayó a n=143. La regla quedó inadjudicable. Era calculable antes.

---

## 2. Lo que se cerró esta sesión

### 2.1 Movers — la asimetría de volatilidad (`banco/movers.py`)

La pregunta "qué tienen en común las que más se mueven" **tiene respuesta y replica**:
volatilidad y rango previos. `atr_24 alto` da lift **1,68×** sobre 44 ventanas rodantes,
aguanta concentración (1,56× sin top-3) y funciona en **89% de las ventanas**;
`rango_168 alto` en 43 de 44. Contra un null de permutación look-elsewhere (p95 = 1,19×),
**16 de 26 features lo baten**. No es ruido.

**Pero apunta al lado equivocado.** Con `atr_24` en el quintil alto: 17,3% de chance de
estar en el decil superior de subida contra **24,0% en el decil inferior de bajada**
(ratio 0,72). La foto de jul-ago 2026 lo confirmó sola: las 19 que más cayeron venían
*más* volátiles que las 19 que más subieron.

La condicional (¿algo separa arriba de abajo dentro del bucket volátil?) dio **0 de 26**.
La tasa direccional se mueve apenas 8pp sobre las 26 features mientras las colas se mueven
28pp. Y `corr(P(top mover), retorno mediano) = −0,72`: **cuanto mejor detectás, peor te va.**

La escalera de quintiles (`--escalera`) mostró que **Q1, el más calmo, es el único con EV
positivo** (+0,218%/trade contando honestamente los timeouts). Pero: 49% de semanas
(requisito 60%), p bloques 0,5995, y +2,6%/año — debajo del piso de stablecoins.

→ memoria `project-movers-asimetria-volatilidad`

### 2.2 Volumen y flujo — CERRADO (`banco/lote_ancho.py`)

**Hallazgo de infraestructura:** `klines()` se quedaba con `[t,h,l,c]` y **tiraba el
volumen**, el open, el nº de trades y el volumen taker comprador. El banco mató 450+
hipótesis sin haber medido nunca volumen — justo la tesis del screener en vivo.
Destrabado con `klines(..., full=True)`, caché versionado `_v2`.

Medido: **0 de 43 en largo, 0 de 43 en corto**. El flujo agresor (`taker`), que era la
apuesta preregistrada por ser la única feature direccional de una vela, dio entre −1,38pp
y +1,38pp con p bloques ≥ 0,42, y **perfectamente antisimétrico** entre largo y corto: cero
información con el signo dado vuelta.

→ memoria `project-volumen-y-flujo-cerrado`

### 2.3 La brecha replay-vivo — DIAGNOSTICADA (`fade/PUENTE2.md`)

No es cadencia ni universo. Medido sobre `screener_pairs_snapshot`: producción corre cada
**2,00 min** con **191-223 pares**, o sea `--max-pairs 200` ya era correcto y el "defecto"
que anotó la sesión anterior era falso.

La corrida a 2 min dio **9,1 alertas/día** contra las ~24 que predije — casi idéntico a la
de 5 min (9,7) con 2,5× más escaneos. **El replay satura.** Causa mecánica:
`backtest.py:768` fija `candle_status:"closed"` y cada scan toma la última vela cerrada, así
que dos scans dentro de la misma vela de 5m ven datos idénticos. Y el vivo tiene un detector
que el replay **no tiene**: `EXPLOSION_FORMING` (`screener.py:717-762`) corre sobre velas de
5m en progreso. La composición lo confirma: EXPLOSION/BREAKOUT = **1,39 en vivo, 0,81 en el
replay**.

**Corolario que aplica a todo el repo:** ningún `--scan-interval-min` cierra esto. Comparar
dos configs entre sí sigue siendo válido (misma población sesgada para ambas), pero **todo
número absoluto del backtest es incomparable con el vivo**.

→ memoria `project-replay-brecha-diagnosticada`

### 2.4 Dos defectos de infraestructura encontrados de paso

- **`screener_pairs_snapshot` se purga a ~30 días** (arranca 2026-07-25). El CLAUDE.md del
  swing dice que "conserva histórico" — **es falso**. Con la key seteada devuelve 0 para
  fechas viejas y el backtest cae al fallback sin avisar que el motivo es retención.
  (Memoria `project-swing-backtest-sesgo-universo` ya corregida.)
- **`SEM_N_MIN = 20` hace inaplicable la compuerta de semanas a eventos esparcidos.** Con
  ~1.040 eventos en 5,6 años (~3,5/semana) ninguna semana llega al mínimo y la compuerta
  sale `--` en todas las hipótesis: no falla, no se puede evaluar. **Cualquier lote de
  eventos raros en este banco tiene el mismo agujero.** Hay que reemplazarla por bloques
  temporales antes del próximo estudio de evento.

---

## 3. ~~EL HILO ABIERTO~~ — unlocks, CERRADO el 2026-08-25 (corrida 2)

### 3.1 Qué está construido y funciona

- **`banco/unlocks.py`** — capa de datos. `api.llama.fi/emissions` pasó a plan pago (402),
  pero **la CDN sigue gratis y abierta**: `defillama-datasets.llama.fi/emissionsProtocolsList`
  y `/emissions/{slug}`. Baja los 370 protocolos y cachea ya extraído (KB en vez de 2 MB).
  **121 protocolos con par USDT en Binance.** `py -3.13 -u unlocks.py` imprime el censo.
- **`banco/test_unlocks.py`** — el test. El diseño clave: en vez de armar una tabla de
  eventos aparte, genera la tabla de primer toque **normal** sobre las monedas con vesting
  y **marca** las entradas dentro de las 12h posteriores a un desbloqueo. Así
  `wr_pareado()` hace el control mismo-símbolo solo. Eso es esencial: sin él el test mide
  "las alts con vesting bajan", que es un hecho de la muestra, no del desbloqueo.
- **`banco/PREREGISTRO_UNLOCKS.md`** — preregistro + resultados, sin editar lo de arriba.

### 3.2 Dónde quedó exactamente

**NO adjudicable.** La regla de parada se apoyaba en el bucket `>=10%`, que quedó con
**n=143** (< 200). Cuatro de los cinco buckets subpotenciados.

Desglose de los 2.431 eventos agregados: 478 anteriores a la primera vela del par
(irrecuperables), **913 futuros** (el calendario llega a 2032+ y no filtré por fecha
pasada), **1.040 usables**. La muestra se agotó — el techo es de datos disponibles.

**Lo que sí se midió apunta EN CONTRA de H.** Contra la línea base de la misma moneda:

| hipótesis | n | largo vs pareado | sin top-3 | p bloques |
|---|---|---|---|---|
| dosis 2–5% | 341 | **+5,3pp** | −0,2 | 1,0000 |
| `noncirculating` | 238 | +5,1pp | −2,0 | 1,0000 |
| `insiders` | 273 | +4,1pp | −1,7 | 1,0000 |
| todos >=0,5% | 1.036 | +1,5pp | −3,3 | 1,0000 |
| `privateSale` | 237 | −1,4pp (único en dirección de H) | −7,6 | 1,0000 |

Todas mueren igual: `sin_top3` da vuelta el signo, p bloques 1,0000. Es concentración.

### 3.3 Se eligió A, se corrió, y la familia quedó CERRADA

`banco/PREREGISTRO_UNLOCKS_2.md` — preregistro de tendencia continua con la
**contaminación declarada en el encabezado** y test **two-sided**. Corrida:
`py -3.13 -u test_unlocks.py --tendencia` (log `unlocks_tendencia.log`).

**beta = −5,91pp de win rate por década de dosis** (n=1.036, 78 símbolos). 4 de 6
compuertas pasan; fallan las dos de significancia: **p permutación 0,1093** e **IC95 por
bootstrap de símbolos [−12,76 , +0,83]** (toca cero). El IC trimestral **sí** excluye cero
[−10,23 , −0,65]: el efecto es consistente en el TIEMPO pero no entre NOMBRES.

Aguanta lo que mató a la corrida 1 — sin top-3 −3,93pp, sin top-1 −5,74pp, mismo signo en
las dos épocas — y el **placebo a −30 días da −0,77pp con p=0,76**, o sea el control
negativo funciona y el cableado es correcto. Pero el efecto es más chico que lo que 1.040
eventos distinguen de cero, y 1.040 es el techo de la data.

**El giro que importa:** la corrida 1 (buckets) insinuaba +5,3pp, o sea CONTRA H original.
La tendencia dio **negativo**, la dirección de H. No es contradicción: la corrida 1
comparaba buckets entre monedas y la pendiente se identifica **51% dentro de cada moneda**.
El bucket que contaminó el prior era ruido de composición. **Escribir el test two-sided fue
lo que salvó la corrida** — one-sided en la dirección contaminada habría leído esto como
refutación.

**Y esta vez el cierre no es "subpotenciado":** el MDE se fijó en 6,6pp por década ANTES
de estimar nada, con la nula por permutación. Regla de método nueva, que se suma a la de
contar el n post-join:

> **Calcular el MDE con la nula real antes de estimar el efecto.** Permutar da sd(beta)
> sin revelar el resultado, y convierte un "no se pudo medir" en "no está".

**Lo reutilizable:** `preparar()` + `_p_permutacion()` + `_ic_bootstrap()` en
`test_unlocks.py` sirven para **cualquier** estudio de evento esparcido — que es lo que
queda vivo de esa familia (anuncios de listado, flujos on-chain).

## 4. Otros hilos abiertos

### 4.1 ~~Cablear el detector forming al backtest~~ — HECHO el 2026-08-26

`EXPLOSION_FORMING` corre ahora **dentro del loop de simulación**. Detalle completo en la
adenda de `fade/PUENTE2.md`. Resumen:

- `backtest.py:build_forming_features()` arma la vela de 5m en progreso desde 1m; el
  bloque "EXPLOSION sobre vela FORMING" en `classify()` es espejo del de `screener.py`.
  El 1m viaja dentro de `klines[sym]["1m"]` → **ninguna firma cambió**. `--no-forming-1m`
  lo apaga.
- **Validado 4 veces**: espejo del screener (305 instantes, dif 1,4e-11), **sin lookahead**
  (243/243 idénticas corrompiendo todo el futuro), **regresión idéntica a HEAD** con el
  detector apagado (94 vs 94 alertas), y A/B quirúrgico (sólo cambia EXPLOSION).
- **A/B 200 pares / 1 semana / scan 2 min**: EXPLOSION 51 → **94** (+84%),
  EXPLOSION/BREAKOUT 0,35 → **0,64** (×1,84). 56 alertas forming sobre 37 símbolos.

**Tres trampas encontradas, las tres silenciosas:**

1. **`ta` usa `ddof=0` en Bollinger**, no el default de pandas → con `ddof=1` había un
   sesgo constante del 2,6% en una sola dirección, sin que nada fallara.
2. **Con `--scan-interval-min` múltiplo de 5 el detector no dispara NUNCA** (todos los
   scans caen en el borde de la vela). Con el default de 15, cablearlo "no cambia nada" y
   la conclusión sería falsa. Producción corre cada 2 min. Hay aviso en `main()`.
3. **`_analyze_key()` no incluía `EXPLOSION_FORMING`** y la inyección es del primer pase →
   un `--compare` entre cfg con y sin forming reutilizaba candidatos: falso en silencio.

**LO QUE FALTA, y es lo único:** el A/B sobre la ventana de 8 semanas que midió el vivo,
para comparar contra el 1,39 con el mismo denominador. Se lanzó y se detuvo a poco de
arrancar. El comando:

```bash
py -3.13 -u backtest.py --weeks 8 --end-date 2026-08-15 --max-pairs 200     --scan-interval-min 2 --out bt_p2_on.json          # y otra vez con --no-forming-1m
```

> ⚠️ **No comparar el 0,64 de la corrida de 1 semana contra el 1,39 del vivo.** Son
> ventanas distintas y la composición depende del período: `puente2.json` cubre 56 días y
> da ratio 0,84, la corrida de 1 semana da 0,35. Lo único comparable entre ventanas son
> multiplicadores: el cableado entrega **×1,84** y haría falta **×1,66** (0,84 → 1,39).
> Del orden correcto, pero **no demostrado**.

**Lo que este cableado NO arregla:** el replay sigue viendo sólo velas cerradas para todo
lo demás, así que la saturación por cadencia (2 min = 5 min) queda igual.

### 4.2 Familias que nunca se tocaron

Del brainstorm del 2026-08-24, las que **no comparten causa de muerte** con lo ya cerrado:

- **Microestructura intra-vela** — usar velas de 5m/1m para caracterizar la *forma* del
  camino dentro de la hora, no agregados. El loader ancho (`full=True`) ya lo destraba.
- **Lead-lag entre monedas** — ¿unas se mueven sistemáticamente antes que otras? Todo lo
  probado es serie de tiempo por símbolo o transversal contemporáneo.
- **Eventos programados que no sean unlocks** — anuncios de listado en Binance (timestamp
  exacto, efecto documentado), flujos on-chain hacia/desde exchanges.
- **La cola ilíquida** — todo se midió sobre `base200` (top volumen), donde la competencia
  es máxima. Mecánicamente más plausible abajo, **pero el modelo de costos (0,20% sin
  slippage) está mal ahí** y hay que rehacerlo antes de creerle a nada.

### 4.3 Lo que NO hay que volver a proponer

Precio en todas sus transformaciones (450+ hipótesis), régimen (7 detectores × 22
trimestres), salida/timing (7 confirmaciones: mueven mediana y cola, nunca la media),
straddles (sin convexidad con órdenes stop), ML sobre las 36 features viejas (techo
condicional medido), volumen y forma de vela (esta sesión, 0/86).

---

## 5. Inventario

**Creados esta sesión:**

```
banco/movers.py              foto / estudio / condicional / escalera
banco/movers_estudio.csv     26 features × 44 ventanas
banco/movers_cond_atr.csv    condicional dentro del bucket volátil
banco/lote_ancho.py          familias volumen/forma/transversal/ciclo de vida
banco/lote_ancho.csv|.log    0 de 86
banco/PREREGISTRO_ANCHO.md   OOS 2024-08→2025-08 declarada y NO usada (nada que promover)
banco/unlocks.py             capa de datos DefiLlama (CDN gratis)
banco/unlocks_eventos.csv    22.508 eventos, 121 pares
banco/test_unlocks.py        el test con control pareado
banco/unlocks_resultado.csv  + unlocks_test.log
banco/PREREGISTRO_UNLOCKS.md preregistro + resultados
fade/PUENTE2.md              preregistro + diagnóstico de la brecha
fade/puente2.json|.log       replay a cadencia 2 min
```

**Modificado:** `banco/klines.py` — `klines(..., full=True)` y `load_panel(..., full=True)`
conservan open/volumen/quote/trades/taker, caché `_v2` separado. Compatible hacia atrás.

**Memorias escritas:** `project-movers-asimetria-volatilidad`,
`project-volumen-y-flujo-cerrado`, `project-replay-brecha-diagnosticada`,
`project-unlocks-primera-corrida`. **Actualizada:** `project-swing-backtest-sesgo-universo`
(retención de 30 días del snapshot).

**Ventana OOS virgen:** 2024-08-01 → 2025-08-01. Declarada en `PREREGISTRO_ANCHO.md` y
nunca mirada, porque no sobrevivió nada que promover. **Sigue disponible.**

---

## 6. Por donde empezar

1. ~~Unlocks: decidir A o B.~~ **HECHO** — se eligio A, se corrio, familia **cerrada**
   (seccion 3.3). El limite es de datos y no se mueve; no volver a proponerla.
2. ~~Arreglar `SEM_N_MIN`.~~ **HECHO** (`f4ce68d`). Para eventos esparcidos la salida no
   es aflojar el minimo sino **bloques temporales**, como hizo la corrida 2 de unlocks.
3. ~~Microestructura intra-vela.~~ **HECHO, y tambien cerrada** — ver seccion 7.
4. ~~4.1, cablear el detector forming.~~ **HECHO** (seccion 4.1). Queda **una sola
   corrida** para adjudicarlo: el A/B a 8 semanas sobre la ventana del vivo, con el
   comando ya escrito ahi.
5. **Lo que queda sin tocar** (seccion 4.2): lead-lag entre monedas, eventos de listado en
   Binance, y la cola iliquida (costos rehechos primero).

---

## 7. Microestructura intra-vela — CERRADA el 2026-08-25

`banco/PREREGISTRO_MICRO.md` (ciego, a diferencia del de unlocks 2), `banco/micro.py`,
`micro.csv`, `micro.log`. 12 features imposibles de calcular con velas de 1h (forma del
camino dentro de la hora + tres de liquidez: Amihud a 5m, autocorrelacion lag-1, spread de
Roll), x2 suavizados x2 colas x2 versiones x2 direcciones = **192 hipotesis**, sobre
111.330 entradas de 187 pares.

**0 sobrevivientes.** Pero **65 cruzan el umbral y las 65 mueren en el mismo lugar**: la
correccion por multiplicidad, porque el p por bloques dice que no hay nada.

> **37 hipotesis con p_indep < 0,001. CERO con p_bloques < 0,05.**
> La mejor: **p_indep 1,1e-36 y p_bloques 0,3845**.

Es el caso mas extremo que dio el repo de la brecha que ya conocia. Y la compuerta semanal
dice lo mismo: la mejor consistencia es 0,592 y la mediana 0,531, contra 0,60. El margen
de +4pp viene de pocas semanas gordas.

**Lo que SI quedo demostrado:** la forma del camino intra-hora **no es volatilidad
disfrazada**. Ese era el riesgo preregistrado, y el control lo descarto: de las 19 crudas
con margen >+1pp, la version condicionada al quintil de `atr_24` **retiene el 94%**, y
`amihud_24 bajo` mejora de +3,42 a +4,36. Es informacion distinta; lo que le falta es
consistencia temporal. (El lado largo si era vol disfrazada; el corto no.)

**La historia coherente que no sirve, anotada para no redescubrirla:** las 8 mejores son
todas del lado corto — `amihud bajo`, `tk_sd bajo`, `hhi bajo` → **las horas ordenadas y
liquidas preceden caidas**. Sobrevive el pareado (+3,25pp), o sea no es seleccion de moneda
ni deriva. Vive en pocas semanas.

**Lo que NO cierra:** la **cola iliquida**. Amihud, Roll y `ac1_5m` son mecanicamente mas
grandes abajo de `base200`; que no aparezca arriba no dice que no exista abajo. Pero hay
que **rehacer el modelo de costos primero** — 0,20% sin slippage esta mal justo ahi.

**Infraestructura que quedo lista:**
- `klines.load_panel(..., workers=N)` — descarga paralela. El cache de **5m de base200
  (2025-08→2026-08) ya esta bajado: 1,7 GB**, o sea la proxima corrida sobre 5m es gratis.
- `micro.py` pasaba 273s por par con `groupby.apply`; vectorizado, **0,10s** (2700x).
- `micro.py --nula N` — nula de look-elsewhere por **desplazamiento circular de las
  features dentro de cada simbolo** (conserva la autocorrelacion de ambos lados, rompe
  solo la alineacion). Dio 0 de 192 en 5 repeticiones. **Reutilizable en cualquier lote**,
  y es mas honesta que permutar filas.

**OOS 2024-08 → 2025-08: sigue virgen.** No se uso, porque no hubo nada que promover.

---

**Lo que este repo mide, dicho sin vueltas:** el mercado es eficiente respecto de la
información que hay en el precio, en las 200 monedas más líquidas, a horizonte de días a
semanas. Eso no es un fracaso del método — es el resultado correcto para el segmento más
competido. Cambiar la respuesta requiere cambiar **la información** (datos que no salgan
del precio), **el terreno** (la cola ilíquida, con costos rehechos) o **el juego** (dejar de
predecir dirección). Iterar más sobre lo mismo sube la vara del azar, no baja la del hallazgo.
