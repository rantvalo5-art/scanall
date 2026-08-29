# HANDOFF — las tres direcciones que quedan

> Escrito el **2026-08-28** al cerrar la sesión de las corridas 5, 5b, 6 y 7.
> **Este es el punto de entrada.** `HANDOFF_SIGUIENTE.md` pasa a ser el registro completo
> de lo cerrado (sus §0.5 a §0.8 tienen el detalle de esta sesión); los anteriores
> (`HANDOFF_CIERRE.md`, `HANDOFF_SENALES.md`, `HANDOFF_BASIS.md`) son históricos.
>
> Se abre en frío: la §0 es operativa, la §1 dice dónde estamos, la §2 son las tres
> direcciones, la §3 las reglas que no se negocian.

---

## 0. Cómo arrancar (verificado, no de memoria)

```bash
cd banco
py -3.13 -u ranking.py --nula                  # el harness bueno: MDE primero
py -3.13 -u correr_onchain.py --nula           # patrón de "contar el n ANTES"
py -3.13 -u libro_perp.py --top 200 --reps 3   # costos reales, spot y perp
py -3.13 -u correr_velas.py --tf 1d            # eventos con control por barra
```

**Gotchas, todos pisados:**

- Siempre `py -3.13`, nunca `python`. Siempre `-u`.
- `$env:PYTHONIOENCODING = "utf-8"` antes de correr, o revienta con cp1252.
- Los procesos se llaman `python3.13`. **`ps` de Git Bash NO los ve** — usar
  `Get-Process python3.13` desde PowerShell. Un `ps` vacío no significa que murió.
- Corridas pesadas **de a una**. La de velas a 1h son ~54 min.
- **Heredocs de bash con tildes/backticks fallan.** Escribir archivos con la herramienta
  de escritura directa, o con un script de Python.
- **`ta` calcula Bollinger con `ddof=0`.** Verificar numéricamente antes de reimplementar
  cualquier indicador suyo.
- **Nunca `groupby.apply` con lambdas** sobre miles de grupos (2700× de diferencia
  medida).
- El writer de parquet falla en esta máquina: el caché del banco cae a `.csv`.

**Cachés (grandes, no borrar, todos gitignoreados):** `banco/.kline_cache/` (1,7 GB, ahora
con **1d y perp**), `.backtest_cache/` (1,77 GB), `banco/.funding_cache/`,
`.metrics_cache/`, `.unlock_cache/`, **`.onchain_cache/`**.

**Rama:** `banco/primer-toque`. ⚠️ Arrastra 3 commits que tocan el swing en producción
(`swing/backtest.py` +753 líneas, `screener.py`, `config.json`). Si se mergea, se revisan
aparte. `main` tiene el radar corriendo y no toca nada de esto.

---

## 1. Dónde estamos

### 1.1 Las tres cosas con FECHA (esto es lo único que corre solo)

| fecha | qué | comando |
|---|---|---|
| **~8 sep** | primer chequeo del radar | `cd radar && py -3.13 -u medir.py` |
| **~14 oct** | el chequeo que decide para un efecto de la mitad | idem |
| **19 oct** | forward test del **fade** (`HANDOFF_CIERRE.md`) | `cd fade && py -3.13 evaluar.py` |

`radar/HANDOFF_FORWARD_TEST.md` tiene la decisión de cada resultado, escrita antes de que
existiera un dato. **No aflojarla.**

### 1.2 El mapa, comprimido

Diez familias estándar de confirmación de dirección. **Nueve medidas, todas en cero:**

| familia | veredicto |
|---|---|
| Momentum transversal | 4.140 hipótesis, **0** |
| Momentum de serie / trend | ingredientes medidos, **0** |
| Reversión / fade | **la única viva** — forward test 19-oct |
| Carry (funding) | **0**, como costo y como señal |
| Order flow / microestructura | 0 de 192; maker cerrado por comisión |
| Posicionamiento / contrarian | 0 de 276 |
| Régimen | 7 detectores × 22 trimestres, ninguno gateable |
| On-chain | 0 de 420 (corrida 6) |
| Estudios de evento | unlocks cerrado; **listados = 2.3** |
| **Patrones de velas** | 0 de 660 (corrida 7) — la canónica acierta **50,0%** |
| ML / no lineal | techo condicional medido |
| **Patrones de gráfico** | **← nunca tocada (2.1)** |

**Lo único que funcionó, dos veces, es no direccional:**
- **Vender volatilidad** (+20,96%/año, 5,3 años, mecanismo real). Murió **compitiéndose**,
  no siendo falsa: +7,33%/año en el régimen reciente, dentro del piso de stablecoins.
- **Magnitud** (el radar): sobrevive en **cinco** diseños distintos y cuatro regímenes.
  Sigue **sin instrumento** para cobrarse.

### 1.3 Lo que la sesión del 28-ago agregó al método

Cuatro cosas que cambian cómo se corre lo que sigue:

1. **El universo se filtra por CLASE DE ACTIVO, no solo por volumen.** `base200` arrastra
   7 stablecoins/FX y 9 acciones tokenizadas: 9,1% del universo que se lleva **37-44% de
   los cupos** de cualquier ranking que elija "lo quieto". Usar la lista `FUERA` de
   `correr_velas.py`.
2. **Cuando cambian tres cosas a la vez, se corren los paneles intermedios.** Un salto de
   +0,41 ATR parecía ser el instrumento y era el universo.
3. **La reserva OOS 2024-08 → 2025-08 ya NO está virgen** para el ranking transversal
   (corridas 3, 4 y 6 la contienen). Sigue disponible solo para `lote.py` y
   `lote_ancho.py`.
4. **La tabla de costos de spot subestima al perpetuo por 2×.** Perp: 12-18 bps a $1k,
   15-32 bps a $10k. Spot: 24-34 y 30-62. Medido en el mismo instante, apareado por
   símbolo (`libro_perp.py`).

---

## 2. LAS TRES DIRECCIONES

Ordenadas por **lo que yo haría**, y el criterio no es el prior: es **cuánto cuesta
cerrarlas**. Las tres tienen prior bajo. La primera se cierra o se abre en una tarde; la
última necesita construir un detector.

---

### 2.1 Volatilidad de alts — ★ la primera, y puede terminar en 20 minutos

**La idea.** Vender volatilidad es **lo único que este repo encontró que funcionó de
verdad**: +20,96%/año en BTC, 5,3 años, DD máx −11,3%, sobrevive sacar los 3 mejores
meses, y tiene **mecanismo** (prima de seguro), no es un patrón encontrado buscando. Se
midió **solo en BTC/ETH**, donde Deribit es eficiente y el premio ya se arbitró. En alts
la competencia es mucho menor, así que la prima debería estar menos comprimida.

Es la única dirección que **extiende lo que funcionó** en vez de buscar una feature más.

**Lo que la mata, y hay que medirlo PRIMERO: puede que el instrumento no exista.**

> **Primer paso, y es de VIABILIDAD, no de estadística.** Averiguar si hay algún mercado
> de opciones de alts con volumen real. Deribit listó algunas más allá de BTC/ETH; hay que
> mirar también OKX y Bybit. Lo que se necesita: **open interest y volumen diario por
> subyacente**, y el spread bid-ask de las opciones at-the-money.
>
> **Regla de parada, escribirla antes de mirar:** si no hay al menos **3 subyacentes que
> no sean BTC/ETH** con volumen diario consistente y un spread ATM que deje margen sobre
> el premio esperado, **se cierra ahí**. No se gasta una sesión midiendo una prima que no
> se puede cobrar.

**Si el instrumento existe**, recién ahí se mide la prima, y el harness ya está:
`opciones/iv_rv.py` compara implícita contra **realizada de los 30 días siguientes** (que
es contra lo que se cobra de verdad, no contra la realizada pasada). Su veredicto para
BTC/ETH está adentro del archivo, con el número que lo cerró.

**Y una advertencia que ya está pagada:** sintetizar la venta de volatilidad **con órdenes
stop está cerrado** — regalás k·ATR por trade y falta la convexidad. Si no hay opciones,
no hay atajo.

**Costo:** viabilidad, muy bajo. Medición, bajo (el harness existe).
**Prior:** el más alto de los tres, condicional a que exista el instrumento.

---

### 2.2 Eventos de listado en Binance

**La idea.** Un listado tiene **timestamp exacto**, efecto documentado en la literatura, y
**no sale del precio**. La maquinaria de estudio de evento esparcido sirve tal cual:
`banco/test_unlocks.py` (`preparar()`, `_p_permutacion()`, `_ic_bootstrap()`).

**En contra, y es lo mismo que mató a unlocks:** no hay endpoint (se scrapea el blog de
anuncios de Binance o se usa un dataset de terceros) y **el n va a ser chico** — pocos
listados por mes.

> **Antes de escribir una sola regla: contar el n POST-JOIN y calcular el MDE con la nula
> real.** Es lo que convirtió un "no se pudo medir" en un "no está" en unlocks (1.040
> eventos, MDE 6,6 pp/década) y en la cola ilíquida, y lo que dejó a la corrida 6 poder
> concluir en serio (41 activos, 257 semanas, MDE ±0,065 — igual que el que cerró
> derivados).
>
> **Regla de parada:** si el n post-join no alcanza un MDE comparable al de las corridas
> que sí concluyeron, el veredicto es **"no se pudo medir"** y se cierra sin gastar más.
> `correr_onchain.py --nula` es el patrón exacto a copiar.

**Un detalle que importa y que la corrida 7 dejó armado:** el estimador correcto para un
evento es el de `correr_velas.py` — **control POR BARRA**, no por símbolo. Un listado
ocurre en un momento del mercado, y si el mercado subía ese día, el evento "funciona" sin
que el listado aporte nada. `lote.py` aparea por símbolo y nunca neutralizó ese término.

**Costo:** medio, y casi todo es adquisición de datos.
**Prior:** bajo-medio. El efecto está documentado, pero es el más publicado del mundo y
lleva años de arbitraje.

---

### 2.3 Patrones de gráfico — la décima familia, la única sin tocar

**La idea.** Hombro-cabeza-hombro, triángulos, banderas, cuñas, dobles techos y pisos.
Es la **única familia estándar de confirmación de dirección que el repo nunca midió**, y
quedó explícitamente fuera del alcance de la corrida 7 (§6 de `PREREGISTRO_VELAS.md`).

**Por qué no es lo mismo que las velas.** Un patrón de velas es una conjunción sobre 2-3
barras — una función local del OHLC. Un patrón de gráfico es **estructura sobre decenas de
barras**: requiere detectar pivotes, ajustar líneas de tendencia, y decidir tolerancias.
Esa forma funcional no está en ninguna de las corridas anteriores.

**Por qué va última, y hay que ser honesto:**

1. **Hay que construir el detector**, y ahí vive el peligro. Cada patrón tiene ~4
   parámetros libres (cuántas barras de lookback, qué tolerancia para "dos techos
   iguales", cuánto puede desviarse la línea, qué cuenta como ruptura). **Eso es una
   máquina de fabricar falsos positivos**, y hay que preregistrarlos igual que se
   preregistraron los umbrales de `velas.py`.
2. **El prior bajó** después de la corrida 7. La familia adyacente —la que comparte la
   premisa de que la forma del gráfico anticipa dirección— dio la dirección canónica
   acertando **exactamente 50,0%** contra su propio espejo.
3. **Es subjetiva por construcción.** Dos implementaciones razonables de "hombro-cabeza-
   hombro" detectan conjuntos distintos, y no hay una definición canónica como la hay para
   un envolvente.

**Cómo lo haría, si se hace:**

- **Preregistrar los parámetros del detector ANTES**, con la misma disciplina que
  `velas.py`: cada tolerancia escrita en el código con su valor canónico y sin tocarla
  después. Si un umbral se ajusta viendo el resultado, la corrida no vale.
- **Declarar la dirección de cada patrón antes de medir.** La corrida 7 mostró por qué:
  tres de sus cinco mejores brazos estaban *invertidos*, y elegido el signo después, los
  cinco contaban como aciertos.
- **Estimador con control por barra**, reutilizando `correr_velas.py` tal cual: solo hay
  que reemplazar `velas.patrones(df)` por el detector nuevo. Todo lo demás —compuertas,
  FDR sobre el lote entero, `sin_top3`, bootstrap de bloques semanales, el umbral de "no
  se pudo medir"— ya está escrito y validado.
- **Dos resoluciones, 1d y 1h**, por la misma razón: los patrones de gráfico se definieron
  en diarias.
- **Un control obligatorio:** un detector con **los mismos parámetros pero pivotes
  barajados**. Si el patrón real no se separa de su versión con la estructura destruida,
  lo que se detectó es ruido con forma.

**Costo:** el más alto de los tres. El detector son ~300 líneas y el diseño de sus
parámetros es la mitad del trabajo.
**Prior:** el más bajo de los tres.

---

## 3. Las reglas que no se negocian

> **La regla de parada se escribe ANTES de mirar.** Si se afloja después de ver un número,
> el experimento no vale.

> **Contar el n POST-JOIN y calcular el MDE con la nula real ANTES de estimar.** Convierte
> un "no se pudo medir" en un "no está". Cerró unlocks, la cola ilíquida, y habilitó la
> conclusión de la corrida 6.

> **La dirección de una hipótesis se declara antes de medirla.** Si se elige el signo
> después, todo acierta. Medido: 3 de los 5 mejores brazos de la corrida 7 estaban
> invertidos respecto de lo que el patrón afirma.

> **El p que decide es el de BLOQUES**, no el binomial. La brecha entre los dos *es* el
> autoengaño. Casos medidos: micro 1,1e-36 → 0,3845; lead-lag 2,3e-44 → 0,1735.

> **El n efectivo son las SEMANAS, no las entradas.**

> **El control va POR BARRA, no por símbolo.** El término de mercado es el sesgo principal
> de cualquier test de eventos o rankings, y aparear por símbolo no lo toca.

> **`sin_top3` antes que nada.** En unlocks, las 4 hipótesis con efecto visible dieron
> vuelta el signo al sacar 3 símbolos.

> **Todo se corre a DOS costos.** Un sobreviviente que solo vive al costo barato no cuenta
> (corrida 5). Y si el costo no está medido para ese instrumento, **medirlo** antes de
> declarar el cierre (corrida 5b).

> **El universo se filtra por clase de activo, no solo por volumen.**

> **Cuando cambian varias cosas a la vez, se corren los paneles intermedios.**

> **Generar ancho, no pre-filtrar.** Las compuertas son sobre conclusiones, no sobre
> explorar. El prior propio no aporta; el harness mata barato.

---

## 4. Herramientas — qué usar para qué

| archivo | qué hace |
|---|---|
| **`banco/ranking.py`** | **el harness bueno.** Ranking transversal por barra: sin corte pooled, sin trampa de `SEM_N_MIN`, control por barra, sin solape. Seis compuertas + FDR |
| **`banco/correr_velas.py`** | **el harness de EVENTOS.** Control por barra para máscaras booleanas. Para 2.3 solo hay que cambiar el detector |
| `banco/velas.py` | 15 patrones de velas con sus umbrales canónicos fijados. El patrón a copiar para preregistrar un detector |
| `banco/klines.py` | `load_panel(..., tf=, full=, pin=, syms=, mercado=)`. **`mercado="fut"`** trae perpetuos; `tf="1d"` trae diarias |
| `banco/onchain.py` | CoinMetrics Community (gratis, sin key). Une por `AssetEODCompletionTime`: **sin lookahead y sin lag fijo** |
| `banco/futuros.py` | funding alineado al tablero. Entra en el **retorno**, no en el costo |
| **`banco/libro_perp.py`** | **costos reales del libro**, spot y perp en el mismo instante y apareados |
| `banco/test_unlocks.py` | estudio de evento esparcido: permutación + bootstrap de símbolos. **La base para 2.2** |
| `opciones/iv_rv.py` | DVOL de Deribit vs realizada futura. **La base para 2.1** |
| `banco/correr_onchain.py` | el patrón de `--nula`: contar el n y el MDE antes de nada |
| `banco/libro.py` | camina el libro. `--mercado fut` para perpetuos |

**Los preregistros** (`banco/PREREGISTRO_*.md`) tienen las reglas escritas antes de cada
corrida y los resultados debajo de la línea. `TRANSVERSAL` (4 corridas), `FUTUROS` (5 y
5b), `ONCHAIN` (6), `VELAS` (7).

---

## 5. Lo que este repo mide, dicho sin vueltas

El mercado es eficiente respecto de la información que hay en el precio, en las 200 monedas
más líquidas, a horizontes de horas a semanas. Después de siete corridas y nueve de las
diez familias estándar, eso ya no es un resultado provisorio.

Lo que se agregó esta sesión, y que cierra tres escapes que quedaban abiertos:

- **No es el instrumento.** El perpetuo ordena igual que el spot: +0,004 ATR de diferencia
  media sobre 140 brazos. Abaratar el trading baja la vara, no resucita nada.
- **No es la fuente.** On-chain —la única clase de información que no sale del mercado—
  da 0 de 420 con la misma potencia que cerró derivados.
- **No es la resolución.** A velas diarias, con 253 semanas y un MDE más fino que el de 1h,
  tampoco hay nada.

Y el screener tiene su propio defecto, medido y separado del anterior: **compra +3,12 ATR
después de que el movimiento ya pasó**, y devuelve −0,94 en las 48h siguientes. Ningún
scoring lo arregla, porque el daño es común a todas sus alertas.

**Lo que sí sobrevive todo lo que se le tire es la MAGNITUD** — cinco diseños, cuatro
regímenes, 100% de las semanas en el mejor caso. El problema nunca fue medirla: es que
**no hay instrumento para cobrarla**. Por eso 2.1 es la primera de las tres, y por eso su
primer paso es averiguar si el instrumento existe.
