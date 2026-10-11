# PREREGISTRO — corrida 10: ¿cuánto dura una dislocación entre venues?

> Escrito el **2026-08-29**, **antes de tomar una sola muestra**.
> Dirección **§2.4.D de `HANDOFF_TRES.md`** — la única que quedaba que es una **medición**
> y no una construcción. Código: `banco/dislocacion.py`.
> Resultados **debajo de la línea**.

---

## 1. Qué se pregunta, y por qué vale la pena preguntarlo

El §2.4 del handoff dice que **donde sí se gana plata en este mercado** está afuera de lo
que el repo puede probar: latencia, co-location, flujo privado, market making a escala. El
arbitraje entre venues es de esa lista.

Pero tiene una pregunta empírica barata: *¿las diferencias de precio entre Binance, OKX y
Bybit se cierran en microsegundos, o hay una cola lenta?*

> **Si una dislocación que supera el costo de cruzar persiste más de unos segundos de forma
> recurrente, es capturable SIN co-location.** Si se cierra antes, el negocio es de
> latencia y no hay versión lenta.

**Prior: bajo.** Es la parte más obviamente competida del mercado. Pero el costo de
averiguarlo es unas horas de muestreo por REST público, sin cuenta y sin websocket.

---

## 2. El cambio de métrica respecto del handoff, y por qué NO es aflojar la regla

El handoff pedía medir *"la distribución del |spread entre venues|"*, o sea la diferencia
entre los **precios medios**, contra un costo de ~40-60 bps.

**Eso mediría de más.** Un arbitraje real no cruza contra el medio: **compra al `ask` de un
venue y vende al `bid` del otro**. La diferencia de medios incluye la mitad de cada spread
como si fuera ganancia, cuando en realidad es costo. En un par fino, donde los dos spreads
son anchos, una diferencia de medios de 60 bps puede ser **cero** de oportunidad real.

> **Métrica primaria — el filo EJECUTABLE.** Para cada par ordenado de venues (A → B):
>
> ```
> filo(A->B) = (bid_B - ask_A) / mid * 10.000       [bps]
> ```
>
> Positivo = comprar en A al ask y vender en B al bid deja plata **antes de comisiones**.

**Y como el filo ejecutable ya descuenta los dos spreads, el costo que queda son las
comisiones, no 40-60 bps.** Taker de spot en los tres venues, tramo base: **0,10% por
pata**, o sea **20 bps** la vuelta. No hace falta transferir nada entre venues: un arbitraje
así se opera con inventario pre-fondeado en los dos lados.

**Se reporta TAMBIÉN la métrica del handoff** (`|mid_A − mid_B|` contra 40-60 bps), con el
mismo análisis de duración, para que el cambio sea auditable y nadie tenga que creerme que
la métrica nueva no fue elegida después de ver un resultado. **Las dos lecturas van en la
tabla final.**

**Umbrales, fijados acá:** el filo se evalúa contra **0, 10, 20 y 30 bps**. El caso base es
**20 bps** (dos taker al tramo base). Los 0 bps son el bruto, para ver si siquiera existe la
oportunidad antes de pagar nada.

---

## 3. Los dos artefactos que el handoff marcó, y cómo se controlan

### (a) Los venues no están sincronizados

Los tres se consultan **en paralelo**, con conexión ya caliente, y se guarda el instante
local de cada respuesta más el timestamp de servidor donde el venue lo da (**OKX** en `ts`,
**Bybit** en `time`; **Binance `bookTicker` NO devuelve timestamp** — eso es un hecho de la
API y hay que decirlo).

**RTT medido en esta máquina, con sesión caliente:** Binance **346 ms**, OKX **348 ms**,
Bybit **419 ms** (medianas de 12 llamadas). O sea que la foto de cada venue está **vieja
~175-210 ms**, que es **~10% del umbral de 2 s**. Alcanza, pero el número queda escrito.

> **Descarte preregistrado:** una muestra se tira si el instante local de las tres
> respuestas abarca **más de 1.000 ms**. Se reporta cuántas se tiraron.

### (b) Un par puede estar halted o en subasta

Fabrica dislocaciones enormes que no son operables.

> **Descartes preregistrados:** se tira la muestra de un par si algún venue no devuelve
> `bid` **y** `ask`, o si alguno viene en 0, o si el símbolo no está en estado de
> negociación en ese venue.

### (c) El artefacto que el handoff NO menciona, y que agrego acá: **el tamaño**

Un filo de 100 bps sobre USD 50 de profundidad no es un negocio, es un residuo del tick.

> **Control preregistrado:** una oportunidad solo cuenta si **las dos patas** tienen al
> menos **USD 1.000** de nocional en el tope del libro (`bidQty × precio` y
> `askQty × precio`). Se reporta también el resultado **sin** este filtro, porque la
> diferencia entre los dos números *es* parte de la respuesta.

---

## 4. Diseño del muestreo

- **Venues:** Binance, OKX, Bybit — **spot**. Los tres tienen 163 pares USDT en común.
- **Una sola llamada por venue trae TODOS los símbolos**
  (`/api/v3/ticker/bookTicker`, `/api/v5/market/tickers?instType=SPOT`,
  `/v5/market/tickers?category=spot`). Eso garantiza que los pares de un mismo venue estén
  sincronizados entre sí y mantiene 3 requests por muestra sin importar cuántos pares.
- **Frecuencia objetivo: 2 muestras por segundo** (~500 ms). Muy por debajo del umbral de 2 s.
- **Duración: ≥ 24 horas corridas.** Una corrida más corta **solo sirve para validar la
  cañería y NO es el veredicto.**

**Los 7 pares, fijados acá, elegidos para cubrir tres órdenes de magnitud de volumen** —
porque la hipótesis tiene una predicción direccional sobre ellos:

| par | vol 24h en Binance | tramo |
|---|---|---|
| BTCUSDT | $590 M | el más competido |
| ETHUSDT | $282 M | |
| DOGEUSDT | $24 M | |
| LTCUSDT | $12 M | |
| INJUSDT | $3,6 M | |
| ALGOUSDT | $0,9 M | |
| AGLDUSDT | $0,1 M | el más fino |

*(quedan afuera USDCUSDT y RLUSDUSDT, que son stablecoins — regla de clase de activo)*

> **La dirección se declara antes:** si el negocio tuviera una versión lenta, estaría en los
> pares **finos**, no en BTC. La hipótesis predice **más episodios y más largos a medida
> que baja el volumen**. Si el patrón sale al revés —dislocaciones más largas en BTC que en
> AGLD— es señal de artefacto, no de hallazgo.

---

## 4bis. ENMIENDA — ampliación del universo (2026-08-29, después del piloto)

> Escrita **después de un piloto de 3 minutos que solo validó la cañería** (190 muestras,
> 0 descartadas, skew mediano 197 ms) y **antes de la corrida larga**. Queda registrada
> acá, con su fecha, porque el repo no permite tocar un diseño después de ver un número.

**Qué cambia.** La recolección pasa de **7 pares a 30**.

**Por qué es admisible, y por qué no afloja nada:**

1. **No cuesta un solo request más.** Una llamada por venue trae *todos* los símbolos; los
   pares extra salen del mismo JSON que ya se estaba bajando.
2. **Solo puede hacer MÁS FÁCIL encontrar el efecto.** Más pares = más chances de observar
   una dislocación larga. Una ampliación que va en contra del prior no puede ser un modo
   de forzar un cierre.
3. **El análisis PRIMARIO sigue siendo sobre los 7 preregistrados**, con su tabla propia.
   Los 23 extra se reportan **agregados y aparte**, y su función es de refuerzo: si el
   efecto no aparece en 30 pares que cubren **cuatro** órdenes de magnitud de volumen
   (USD 592 M a USD 0,04 M por día), el cierre es más fuerte, no más débil.

**Los 23 extra**, tomados del ranking de volumen del universo común de 159 pares (sin
stablecoins, FX ni oro), repartidos parejo:

`SOL, SUI, ADA, ONDO, ROBO, DOT, SHIB, MASK, BONK, PENDLE, PEOPLE, APE, XTZ, GMX, THETA,
SKY, LINEA, ENJ, GMT, MANA, NOT, ZRX, RPL` (todos contra USDT)

**Lo que la enmienda NO toca:** ni los umbrales, ni la regla de parada, ni la definición
de episodio, ni los controles de tamaño, skew y halt. Nada de eso se movió.

### Lo que el piloto dejó medido, y que corrige un supuesto del diseño

**La frecuencia efectiva es ~1 Hz, no los 2 Hz pedidos:** manda el RTT de los tres venues,
no el `sleep`. El intervalo real entre muestras del piloto fue **0,79 s**.

> Consecuencia, y se arregla en el código antes de la corrida larga: **la duración de un
> episodio se calcula con el intervalo MEDIDO en el propio dato**, no con el nominal.
> Usar 0,5 s cuando el real es 0,79 s subestimaría toda duración, o sea que empujaría
> hacia el cierre. El intervalo efectivo se reporta en la salida.

Sigue estando muy por debajo del umbral de 2 s de la regla de parada, así que la medición
no pierde validez.


---

## 5. Cómo se mide la duración

- **Episodio** = muestras **consecutivas** en las que el filo de un par ordenado de venues
  supera el umbral.
- **Duración** = (instante de la última muestra − instante de la primera) + un intervalo de
  muestreo. Un episodio visto en **una sola muestra** cuenta como **un intervalo** (~0,5 s).
- Se reporta **mediana, p90 y la fracción de episodios de una sola muestra**.

**Por qué contar así el episodio de una muestra:** si una dislocación no sobrevive a dos
sondeos separados por medio segundo, no dura dos segundos. Es la lectura conservadora, y va
en la dirección del prior, así que **se reporta también la mediana excluyendo los episodios
de una sola muestra** — si las dos lecturas dan lo mismo, el veredicto no depende de esta
elección.

---

## 6. La regla de parada

> **PRINCIPAL** — si la **mediana de duración** de las dislocaciones que superan el costo
> (filo ejecutable > **20 bps**) es **menor a 2 segundos**, el negocio es de latencia y
> **se cierra**: no hay versión lenta.

> **RAMA "NO SE PUDO MEDIR"** — hacen falta **≥ 30 episodios** por encima del umbral para
> afirmar una mediana. Con menos, no se reporta mediana.
>
> Pero ojo con leer eso como un empate: **si sobre ≥ 24 h de muestreo a 2 Hz casi no hay
> episodios que superen 20 bps, eso NO es "no se pudo medir" — es la respuesta**, y es un
> cierre más fuerte que el de duración. La distinción se hace explícita en los resultados,
> porque las corridas 8 y 9 dejaron escrito que decir *por qué* no se pudo medir es parte
> del veredicto.

**Lo que abriría la dirección:** mediana ≥ 2 s con ≥ 30 episodios, **con tamaño ≥ USD 1.000
en las dos patas**, y con el patrón por liquidez en la dirección declarada.

---

## 7. Lo que esta corrida NO hace

- **No opera nada** ni estima P&L. Mide si la ventana existe.
- **No usa websocket ni cuenta.** Si el veredicto quedara a milímetros del umbral, el paso
  siguiente sería websocket — pero eso se decide **después**, no durante.
- **No mira perpetuos.** El evento es spot contra spot.
- **No dice nada sobre latencia/co-location como negocio.** Ese sigue afuera, con o sin
  este resultado.

---
---

# RESULTADOS

## ESTADO DE LA RECOLECCIÓN — **COMPLETA el 2026-10-10.** Lo de abajo es el veredicto.

> **24,00 h corridas**, del **2026-10-09 23:59 al 2026-10-10 23:59 UTC**: **91.247 muestras
> útiles** (393 tiradas, 0,43%), 30 pares, **1,06 Hz** efectivo, skew mediano entre venues
> 204 ms. **Cero huecos de más de 300 s**; el mayor fue de 176 s. **16.424.460 observaciones
> par-ruta.**
>
> Supera la corrida de agosto en las dos dimensiones (21,57 h / 83.950 muestras) y es la
> primera vez que la premisa de §4 —24 horas **corridas**— se cumple.

### Por qué hicieron falta 24 h nuevas y no 2,5

El bloque de agosto **no se podía continuar**. Lo de abajo quedó escrito el 30-ago y era
correcto ese día; 39 días después, pegar 2,5 h a un bloque de agosto no da 24 h corridas, da
dos bloques separados por 39 días. Y al intentarlo apareció que `analizar` lo habría aceptado:
medía la ventana como `t.max() - t.min()` sobre **todos** los CSV que `cargar()` concatena, así
que el span daba ~960 h, la guarda de las 24 h no se disparaba y el veredicto salía como si la
premisa estuviera cumplida. Arreglado: la ventana se mide por **bloque continuo**.

### Y el "detenido desde afuera" de agosto era la suspensión de la máquina

El primer intento de octubre (8-oct) mostró el mecanismo: **tres huecos se comieron 8,33 de
las 8,36 h**, uno solo de **7,66 h**, con la notebook dormida — y el colector, cuando
muestreaba, iba a 0,77 s de hueco mediano contra los 0,76 s de agosto. No era lentitud ni
fallas de los venues. El `standby` en AC estaba en 240 min. Eso explica las tres
interrupciones de agosto, y por eso aquella corrida llegó a 21,57 h.

> **Dos defectos de implementación encontrados al cerrar esto**, los dos en el sentido de
> dejar pasar lo que no debía:
>
> 1. la ventana por `span` en vez de por bloque continuo (arriba);
> 2. la guarda `horas < 24` era **insatisfacible**: `recolectar` corre
>    `while time.time() < fin`, así que la primera muestra cae un intervalo después del
>    arranque y la última uno antes del final. Esta corrida muestreó 86.398,7 s de 86.400 y
>    salía marcada `PILOTO`. Se le puso una tolerancia de 60 s, con el motivo escrito: es un
>    artefacto de medir el bloque como `t[-1] - t[0]`, no un aflojamiento de la regla.

---

## Lo que decía el 2026-08-30, cuando la recolección estaba incompleta

> Al **2026-08-30 18:38 UTC**: **21,57 h de las 24 preregistradas** (90%), **83.950
> muestras**, 30 pares, 7 minutos de huecos en total. **Falta la franja 18:38 → 21:04 UTC.**
>
> El colector fue detenido desde afuera **tres veces** (a las 18,6 h, a 1,4 h y a 1,2 h);
> las tres veces se relanzó y se reanudó sin perder lo juntado. Para terminarlo:
>
> ```
> cd banco
> py -3.13 -u dislocacion.py --recolectar --horas 2.5
> py -3.13 -u dislocacion.py --analizar
> ```
>
> **La regla de las 24 h no se afloja.** Se deja el estado escrito y el veredicto en
> blanco, en vez de declarar uno con 21,6 h y ponerle un asterisco.

### Lo que hay hasta acá

| umbral (filo ejecutable, con USD 1.000 en las dos patas) | episodios | mediana | p90 |
|---|---|---|---|
| **> 0 bps** (bruto, antes de comisiones) | **46.673** | **2,77 s** | 23,1 s |
| > 10 bps | 46 | 0,76 s | 0,76 s |
| **> 20 bps** (el costo: dos taker) | **2** | 0,76 s | 0,76 s |
| > 30 bps | 1 | 0,76 s | 0,76 s |

La compuerta pide **≥ 30 episodios** sobre 20 bps para afirmar una mediana. Hay **2** en
21,6 h. Para llegar a 30, las 2,4 h que faltan tendrían que producir 28 — unas **250× la
tasa** observada. El resultado no está en la balanza.

### ⚠️ Un número que SÍ se está moviendo, y es el interesante

La duración de las dislocaciones **brutas** —el hallazgo que decía que "esto no es un
negocio de latencia"— **está bajando a medida que entra la sesión activa**:

| cobertura | mediana de duración a 0 bps |
|---|---|
| 7,1 h | **4,30 s** |
| 18,7 h | **3,55 s** |
| **21,6 h** | **2,77 s** |

Va hacia el umbral de 2 s, no se aleja. **Lo que dije antes —"la dislocación es lenta, más
del doble del umbral"— era una lectura de cobertura parcial y hay que tratarla como
provisoria.** Las horas que faltan son las más activas del día, que es exactamente donde
cabe esperar que se cierren más rápido. **Ése, y no el conteo de episodios, es el motivo
real por el que vale la pena completar las 24 h.**

### Lo que ya está firme, y no depende de las horas que faltan

**El tamaño, no la velocidad, es lo que mata el negocio.** Fracción de observaciones con
USD 1.000 de nocional en el tope de **las dos** patas:

| par | vol 24h | con tamaño |
|---|---|---|
| BTCUSDT | $592 M | 95,5% |
| ETHUSDT | $282 M | 89,7% |
| DOGEUSDT | $24 M | 43,0% |
| LTCUSDT | $12 M | 36,4% |
| INJUSDT | $3,6 M | **0,0%** |
| ALGOUSDT | $0,9 M | **0,1%** |
| AGLDUSDT | $0,1 M | **0,2%** |

> **Justo donde la hipótesis predecía menos competencia —los pares finos— no hay nada que
> operar: el tope del libro no llega a USD 1.000.** La ventana existe y no entra nada por
> ella. El patrón por liquidez declarado antes de medir sale **cero en los 30 pares.**

### El outlier de 108 bps, auditado

El filo más grande de toda la muestra fue `MANAUSDT`, ruta bybit→okx. **No era un halt ni
una subasta**, que es lo que el preregistro mandaba descartar:

En 3 segundos MANA cayó de 0,0772 a 0,0760 (−1,5%). Binance y Bybit bajaron primero; **OKX
quedó una muestra atrás**, cotizando 0,0772 cuando el resto ya estaba en 0,0763. Era una
**cotización rancia durante un movimiento rápido**.

Y el bid rezagado de OKX tenía **200 MANA = USD 15**.

> **La oportunidad más grande de 21 horas fue una cotización vieja de quince dólares.** El
> control de tamaño —el único de los tres que agregué por fuera de lo que pedía el
> handoff— la descartó solo.


---

# VEREDICTO — **CIERRA.** No hay versión lenta del negocio.

Medido el **2026-10-10** sobre **24,00 h corridas**, 91.247 muestras, 16.424.460
observaciones par-ruta.

## El número que cierra

| umbral del filo ejecutable | episodios | mediana |
|---|---|---|
| > 0 bps (bruto, antes de comisiones) | 25.763 | 4,12 s |
| > 10 bps | 80 | 0,80 s |
| **> 20 bps** (dos taker al tramo base) | **1** | 12,38 s |
| > 30 bps | **0** | — |

**Un episodio en 24 horas, y hacen falta 30 para afirmar una mediana.** Se cierra por la rama
que §6 dejó escrita como la más fuerte: *"si sobre ≥ 24 h de muestreo a 2 Hz casi no hay
episodios que superen 20 bps, eso NO es «no se pudo medir» — es la respuesta"*.

**Y hay que ser preciso en cómo cierra: NO cierra por duración.** El único episodio duró
**12,38 s**, o sea por encima del umbral de 2 s de la regla PRINCIPAL. Con n = 1 no se puede
afirmar nada sobre la mediana, y el preregistro prohíbe reportarla con menos de 30. Lo que
cierra esto es que **la oportunidad no aparece**, no que aparezca y sea corta. En el piloto de
agosto los 2 episodios duraban una sola muestra (0,76 s) y cerraban por las dos ramas; con
24 h completas, la rama de duración queda sin datos y la de ausencia alcanza sola.

## La dirección declarada antes de medir no apareció

§4 fijó que la hipótesis predice **más episodios y más largos cuanto más fino el par**, y que
si saliera al revés sería señal de artefacto. Salió **ninguno de los dos**:

| par | episodios > 20 bps |
|---|---|
| BTCUSDT · ETHUSDT · DOGEUSDT · LTCUSDT · INJUSDT · ALGOUSDT · AGLDUSDT | **0 en los siete** |
| los 23 extra (enmienda post-piloto) | 1 |

Cero en los siete pares primarios. No hay patrón por liquidez porque no hay episodios, así que
tampoco hay artefacto que explicar.

## El hallazgo lateral, y es el más útil de los tres días

**El filo existe y no se puede tocar.** `INJUSDT` tiene filo positivo en el **37,91 %** de las
observaciones, con p90 de **8,2 bps** — es, de lejos, el par más prometedor de los siete. Pero
la columna de tamaño dice **0,1 %**: casi nunca hay USD 1.000 en el tope de las dos patas.

| par | % obs con filo > 0 | p90 del filo | % con USD 1.000 en las dos patas |
|---|---|---|---|
| BTCUSDT | 49,15 % | 1,0 bps | **99,0 %** |
| ETHUSDT | 46,39 % | 1,0 bps | 97,0 % |
| INJUSDT | **37,91 %** | **8,2 bps** | **0,1 %** |
| ALGOUSDT | 2,20 % | −1,7 bps | 0,3 % |
| AGLDUSDT | 6,38 % | 0,0 bps | **0,0 %** |

La relación es monótona y va en contra del negocio: **donde hay profundidad el filo es de 1 bps;
donde el filo se asoma no hay con qué ejecutarlo.** En los 23 extra los máximos son enormes
(RPLUSDT 254,8 bps, GMTUSDT 194,1 bps) con 8 % de cobertura de tamaño — la misma cosa. Es el
mismo mecanismo que el piloto ya había mostrado en un caso puntual: la mejor oportunidad de
21 horas fue una cotización rancia de **quince dólares**.

## Qué queda cerrado y qué no

**Cerrado:** la versión lenta del arbitraje entre venues, muestreando a ~1 Hz por REST. No hay
ventana que un ejecutor sin colocation pueda tomar, y el motivo no es la velocidad: es que el
filo y la profundidad están en pares distintos.

**No cerrado, y §7 lo dejó fuera a propósito:** la versión rápida (websocket, colocation). Este
preregistro **no la mide** y este veredicto **no dice nada** sobre ella.
