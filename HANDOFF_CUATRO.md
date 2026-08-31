# HANDOFF — lo que queda pendiente

> Escrito el **2026-08-30**, al cerrar las corridas 8 a 13.
> **Este es el punto de entrada.** `HANDOFF_TRES.md` pasa a ser el registro de las
> direcciones que se cerraron; los anteriores (`HANDOFF_SIGUIENTE.md`, `HANDOFF_CIERRE.md`,
> `HANDOFF_SENALES.md`, `HANDOFF_BASIS.md`) son históricos.
>
> **No hay ninguna dirección abierta.** Las diez familias estándar están medidas y el
> último hueco de horizonte está cerrado. Lo que queda es **una medición sin terminar y
> tres fechas** — está todo en la §1.
>
> **Antes de proponer cualquier cosa nueva, leer la §2.** Dice con qué precisión mide este
> repo, y esa precisión es peor de lo que las corridas anteriores dejaban sonar.

---

## 0. Cómo arrancar (verificado, no de memoria)

```powershell
$env:PYTHONIOENCODING = "utf-8"
cd C:\Users\asd\Pictures\scanall\banco
py -3.13 -u ranking.py --nula                 # el harness bueno: MDE primero
py -3.13 -u horizonte_largo.py                # MDE anualizado por horizonte + costo
py -3.13 -u potencia_graficos.py --tf 1d      # MDE(tasa, horizonte) con mascaras al azar
py -3.13 -u dislocacion.py --analizar         # lo juntado de la corrida 10
```

**Gotchas, todos pisados:**

- Siempre `py -3.13`, nunca `python`. Siempre `-u`.
- `$env:PYTHONIOENCODING = "utf-8"` antes de correr, o revienta con cp1252.
- Los procesos se llaman `python3.13`. **`ps` de Git Bash NO los ve** — usar
  `Get-Process python3.13` desde PowerShell. Un `ps` vacío no significa que murió.
- **Los procesos largos en segundo plano se mueren.** En esta sesión tres colectores
  fueron detenidos desde afuera (a las 18,6 h, 1,4 h y 1,2 h). **Cualquier cosa que
  recolecte por horas tiene que poder reanudarse**: escribir a disco con `flush` frecuente,
  un archivo por corrida, y que el análisis junte todos los archivos del directorio.
  `dislocacion.py` ya está hecho así.
- **Heredocs de bash con tildes/backticks fallan**, y hay **dos variantes más que costaron
  tiempo**:
  - dentro de un heredoc, `\n` en un string de Python **se convierte en salto de línea
    real** antes de que Python lo vea → un `str.replace` con `\n` en el patrón no matchea.
  - un backtick dentro de `py -3.13 -c "..."` dispara **sustitución de comandos del shell**
    y te borra el texto en silencio.
  - **La salida:** escribir un `.py` en el scratchpad con la herramienta de escritura
    directa y ejecutarlo. Nunca parchear archivos con heredocs.
- **`ta` calcula Bollinger con `ddof=0`.** Verificar numéricamente antes de reimplementar.
- **Nunca `groupby.apply` con lambdas** sobre miles de grupos (2700× medido).
- El writer de parquet falla en esta máquina: el caché del banco cae a `.csv`.
- **Un detector que filtra pivotes con `ph[ph <= tope]` en cada barra es cuadrático.**
  `np.searchsorted` lo baja a O(log n) y da exactamente lo mismo (verificado).

**Gotchas de APIs, los tres dan falsos silenciosos:**

- **OKX**: el tamaño de contrato es **`ctVal` × `ctMult`**, no `ctVal`. Usar `ctVal` solo
  infla el nocional **100×**.
- **Bybit** `/v5/market/historical-volatility` **exige `quoteCoin=USDT`**. Sin eso devuelve
  `retCode: 0, SUCCESS` con **lista vacía**, que se lee como "no hay datos".
- **Binance `exchangeInfo` se filtra por `status`, no por nombre.** Los deslistados vienen
  con `status == "BREAK"`. Filtrar por nombre los da a todos por vivos.

**Y uno que no es de API pero se cobró el token una vez:** nunca imprimir `e` crudo en un
`except` que envuelva una llamada a Telegram — el mensaje de una excepción de `requests`
trae la URL, y la URL de Telegram trae el token en el path. Así se filtró el 2026-08-22, al
log de Actions de un repo público. Se arregló en `swing/screener.py` el 27-ago y **quedaron
tres sitios afuera** (`screener.py`, `swing/exit_tracker.py`, `radar/radar.py`), los tres
corriendo solos; PR #26. Usar `_sin_token()`, que es idéntico en los cuatro archivos. **Un
`except` que no imprime nada tampoco alcanza: si no hay `try`, el traceback lo escribe igual.**

- **`fade/.cache/` no es un scratch: es un ACUMULADOR, y para las fechas viejas es el
  único lugar donde están los datos.** `bajar()` pide solo el tramo nuevo y mergea por
  `id`, así que nunca pierde filas; Supabase sí las pierde, porque `update_outcomes.py`
  purga todos los días lo que pase de `RETENTION_DAYS`. El `.gitignore` de ese directorio
  decía "se regeneran solos" y era falso. Copia del 2026-08-30 en
  `..\scanall_respaldo\fade_cache\`.

**Cachés (grandes, no borrar, todos gitignoreados):** `banco/.kline_cache/` (1,7 GB),
`.backtest_cache/` (1,77 GB), `banco/.funding_cache/`, `.metrics_cache/`, `.unlock_cache/`,
`.onchain_cache/`, **`banco/.listados_cache/`** (diarias de los 734 pares USDT que
existieron, deslistados incluidos), **`banco/.dislocacion/`** (440 MB de libros),
`opciones/.dvol_cache/`, `opciones/.snapshots/`.

**Rama:** `banco/primer-toque`. ⚠️ Arrastra commits que tocan el swing en producción
(`swing/backtest.py`, `screener.py`, `config.json`). Si se mergea, se revisan aparte.
`main` tiene el radar corriendo y no toca nada de esto.

---

## 1. LO PENDIENTE — esto es todo

### 1.1 La única medición sin terminar: corrida 10 (dislocación entre venues)

**Estado: 21,57 h de las 24 preregistradas** (90%), 83.950 muestras, 30 pares, 7 minutos de
huecos en total. **Falta la franja 18:38 → 21:04 UTC.**

```powershell
cd banco
py -3.13 -u dislocacion.py --recolectar --horas 2.5
py -3.13 -u dislocacion.py --analizar
```

Se reanuda solo: cada corrida escribe su CSV y `--analizar` junta todos.

**El veredicto está en blanco a propósito** en `banco/PREREGISTRO_DISLOCACION.md`. La regla
de las 24 h se escribió antes de la primera muestra y **no se afloja después de ver el
número**.

> **Y el motivo para terminarla NO es el conteo de episodios.** Sobre 20 bps hay **2** en
> 21,6 h contra los 30 que pide la compuerta; para llegar, las 2,4 h que faltan tendrían
> que producir 28 (250× la tasa observada). Eso ya está decidido.
>
> **El motivo es un número que se está moviendo.** La duración de las dislocaciones
> **brutas** venía leída como el hallazgo de la corrida ("esto no es un negocio de
> latencia"), y baja a medida que entra la sesión activa: **4,30 s** (7 h) → **3,55 s**
> (18,7 h) → **2,77 s** (21,6 h), contra un umbral de 2 s. Va hacia el umbral, no se aleja.
> Las horas que faltan son las más activas del día. **Ese número decide si el hallazgo
> secundario existe o no.**

**Lo que ya está firme y no depende de las horas que faltan:** el tamaño, no la velocidad,
es lo que mata el negocio. Observaciones con USD 1.000 en el tope de **las dos** patas:
BTC 95,5%, ETH 89,7%, DOGE 43%, LTC 36%, **INJ 0,0%, ALGO 0,1%, AGLD 0,2%**. Justo donde la
hipótesis predecía menos competencia no hay nada que operar. Y la oportunidad más grande de
21 horas fue una cotización rancia de OKX durante una caída de 1,5% en MANA, con **USD 15**
en el bid rezagado.

### 1.2 Las tres fechas (esto corre solo y no hay que tocarlo)

| fecha | qué | comando |
|---|---|---|
| **~8 sep 2026** | primer chequeo del radar | `cd radar && py -3.13 -u medir.py` |
| **~14 oct 2026** | el chequeo que decide para un efecto de la mitad del tamaño | idem |
| **19 oct 2026** | forward test del **fade** (la última hipótesis direccional viva) | `cd fade && py -3.13 evaluar.py` |

El radar necesita `$env:SUPABASE_KEY` antes de correr `medir.py`.

> **Las fechas tenían fecha de vencimiento, y el 2026-08-30 se arregló.**
> `daytrader_outcomes` —la tabla que lee el forward test del fade— se purgaba sola a los
> **90 días** (`update_outcomes.py`, `purge_old`, corre a diario; verificado en el log del
> run `33291504262`). Las alertas del 17-ago se borraban el **~15-nov**, así que el chequeo
> de las 18 semanas (21-dic-2026) y el de las 69 (13-dic-2027) se quedaban **sin datos, en
> silencio y sin que nada fallara**. Se subió a **550 días** (PR #27). Todavía no se había
> purgado nada: la tabla arranca el 25-jun y los 90 días caían el ~23-sep.
>
> **La lección general, que no es sobre esta tabla:** un experimento preregistrado a
> meses o años vista depende de que el dato siga existiendo ese día. Antes de escribir una
> fecha en un handoff, hay que mirar qué la borra — retención, cuota, un cron que se
> apaga solo. Acá el radar tiene la suya anotada en `radar/HANDOFF_FORWARD_TEST.md` §2:
> GitHub deshabilita los cron tras 60 días sin actividad en el repo.

> **`radar/HANDOFF_FORWARD_TEST.md` y `HANDOFF_CIERRE.md` tienen la decisión de cada
> resultado posible, escrita antes de que existiera un dato. NO AFLOJARLAS.** Ese es el
> único valor que tienen: si se reinterpretan en octubre, el experimento no valió nada.

### 1.3 Nada más está abierto

Las diez familias estándar están medidas. El último hueco de horizonte (>1 semana) está
cerrado. **No hay una dirección para elegir.** Si aparece la tentación de agregar una
feature más, leer la §2 primero y después la §6.

---

## 2. CON QUÉ PRECISIÓN MIDE ESTE REPO — leer antes que nada

La corrida 13 midió el MDE **anualizado** del harness transversal, con el costo en la misma
unidad, sobre 5 años y 187 pares:

| horizonte | barras | MDE %/año | costo %/año | **efecto BRUTO detectable** |
|---|---|---|---|---|
| 168h (7d) | 255 | 22,0 | 10,43 | **32,4** |
| 720h (30d) | 58 | 30,2 | 2,43 | **32,6** |
| 2160h (90d) | 19 | 29,8 | 0,81 | **30,6** |

Con el mejor `k` de todos (40, a 30 días): **26,4%/año**.

**Cómo hay que decir el resultado del repo:**

> ✔ **No hay un efecto direccional GRANDE** —del orden de 30%/año bruto o más— en la
> información que está en el precio, en las 200 monedas más líquidas, a horizontes de 4h a
> 90 días.
>
> ✘ **NO está establecido que no haya uno MODESTO.** Un edge real de 8-15%/año **habría
> sido invisible en las trece corridas.**

**Y el motivo no es un defecto del método: es el largo de la muestra.** Con rebalanceo
semanal hay 255 observaciones independientes en 5 años. El error estándar de la media no
baja de ahí **por más features que se prueben**. Para llevar la resolución a 10%/año harían
falta ~7× más observaciones: **~35 años**.

> **Eso no invita a seguir buscando: es lo contrario.** Dice que la pregunta "¿hay un edge
> modesto?" **no tiene respuesta alcanzable con estos datos**, y que agregar features sobre
> la misma ventana no mueve el error estándar ni un poco. Lo único que lo mueve es el
> tiempo.

---

## 3. Lo que se cerró en esta sesión (corridas 8 a 13)

| # | qué | veredicto | por qué importa |
|---|---|---|---|
| **8** | volatilidad de alts | **no se pudo medir** | el instrumento **existe** y cruzar cuesta 1-2% de la prima, pero solo hay 18 meses de implícita. MDE 39%/año; **BTC con la misma ventana da 27,1%** |
| **9** | eventos de listado | **no se pudo medir** | 544 eventos, **266 semanas** (más que la corrida 6, que sí concluyó) y aun así no alcanza: σ = 23,8 pp por evento. **90 años** para detectar 1% |
| **10** | dislocación entre venues | **pendiente (§1.1)** | el tamaño, no la velocidad |
| **11** | compuerta de patrones de gráfico | **PASA** | la primera compuerta que habilita una corrida |
| **12** | patrones de gráfico | **cero MEDIDO** | una **ruptura pelada** le gana a las cinco figuras; los pivotes barajados llegan tan lejos como los reales |
| **13** | horizontes largos | **no se pudo medir** | alargar el horizonte es un **empate**: el costo cae 13× y la precisión cae lo mismo |

El detalle está en `HANDOFF_TRES.md` §2.0.A a §2.0.D y en los preregistros
`banco/PREREGISTRO_{OPCIONES,LISTADOS,DISLOCACION,GRAFICOS,HORIZONTE_LARGO}.md`.

**Las dos formas de morir por potencia, y hay que decir cuál fue:**

- **por n** (corrida 8): se reabre si aparece el dato.
- **por σ** (corrida 9): **no se reabre con más datos**, solo con otro estimador.

---

## 4. Las reglas que no se negocian

> **La regla de parada se escribe ANTES de mirar.** Si se afloja después de ver un número,
> el experimento no vale. Ver §1.1: el veredicto de la corrida 10 quedó en blanco por esto.

> **Contar el n NO alcanza: hay que medir la σ, y decir cuál de las dos falló.** Corrida 6:
> 257 semanas, concluyó. Corrida 9: **266 semanas**, no pudo. Decidía `σ/√n`, no `n`.

> **Todo MDE se reporta ANUALIZADO, no en ATR por tenencia.** Comparar 24h con 90d en "ATR
> por tenencia" no significa nada. Anualizar es lo que dejó ver que el "0 de 4.140"
> descartaba **32%/año bruto y no 10%**.

> **Antes de elegir resolución, calcular el COSTO en las mismas unidades que el ruido.** A
> 1h el costo es 0,155 ATR y el MDE 0,029: la corrida solo podía encontrar un efecto 5×
> más grande que el que su propio ruido permitía detectar.

> **Estirar el horizonte no es una palanca: es un empate.** De 7d a 90d el costo anual cae
> 13× y la precisión cae lo mismo. El bruto detectable se queda en ~31%/año en todo el rango.

> **Un control tiene que poder GANAR, o no es un control.** Una ruptura pelada le ganó a
> las cinco figuras de gráfico. Sin ese control el cero se leía como "el mercado es
> eficiente" en vez de "lo que el patrón detecta es el breakout".

> **El FDR va sobre el LOTE ENTERO, con los controles adentro.** Medido: con 180 brazos una
> **máscara al azar** dio **p = 0,0495** y aguantó `sin_top3`.

> **La dirección de una hipótesis se declara antes de medirla.** Medido dos veces: 3 de los
> 5 mejores brazos de la corrida 7 estaban invertidos, y el brazo más tentador de la
> corrida 12 también.

> **Antes de decir "sumo activos y gano potencia", MEDIR la correlación.** ρ = +0,92 entre
> el P&L de la straddle de BTC, ETH, SOL y XRP: **4 subyacentes son 1,07 independientes.**

> **El p que decide es el de BLOQUES**, no el binomial. El n efectivo son las SEMANAS.

> **El control va POR BARRA, no por símbolo.** `sin_top3` antes que nada. Dos costos siempre.

> **El universo se filtra por clase de activo, y un estudio de eventos por ESTADO, no por
> nombre.** Los deslistados vienen con `status == "BREAK"`; sin ellos, un estudio de
> listados es un falso positivo garantizado.

> **Un número que contradice una estructura de mercado conocida es un bug hasta que se
> demuestre lo contrario.** Delató un error de unidades de 100× en OKX y un filtro mal
> puesto en `exchangeInfo`.

> **Lo que se cobra pago a pago se evalúa con la MEDIA, no con la mediana.**

---

## 5. Herramientas — qué usar para qué

| archivo | qué hace |
|---|---|
| **`banco/ranking.py`** | el harness de RANKINGS. Control por barra, sin solape, seis compuertas + FDR |
| **`banco/correr_velas.py`** | el harness de EVENTOS. `correr_graficos.py` es el ejemplo de cómo enchufarle un detector nuevo |
| **`banco/horizonte_largo.py`** | **MDE anualizado por horizonte, con el costo en la misma unidad.** Correr esto antes de proponer cualquier horizonte |
| **`banco/potencia_graficos.py`** | **MDE(tasa, horizonte) con máscaras al azar.** Dice si una familia se puede medir ANTES de construirle el detector |
| **`banco/correr_listados.py`** | `--nula` mide supervivencia, n post-join, MDE en ATR **y en %**, y los años que harían falta. El patrón para cualquier estudio de evento |
| **`banco/dislocacion.py`** | filo ejecutable entre Binance/OKX/Bybit, con control de tamaño y de skew |
| **`opciones/viabilidad.py`** | foto de los 3 venues de opciones. El patrón de "medir si el instrumento existe ANTES del efecto" |
| **`opciones/potencia.py`** | n, σ, MDE, años necesarios, calibración contra un efecto conocido, y la ρ que decide si poolear sirve |
| `banco/graficos.py` | 5 patrones de gráfico sin lookahead + su versión con **pivotes barajados** |
| `banco/klines.py` | `load_panel(..., tf=, full=, pin=, syms=, mercado=)` |
| `banco/libro_perp.py` | costos reales del libro, spot y perp apareados |
| `banco/test_unlocks.py` | estudio de evento esparcido: permutación + bootstrap |

---

## 6. Si alguien quiere seguir igual — lo que yo haría, y lo que no

**Esto es criterio, no dato.** El dato es la §2.

### Lo que haría, en orden

1. **Terminar la corrida 10** (§1.1). Dos horas y media, cierra el único ítem abierto.
2. **Un cron que guarde la implícita de alts todos los días.** Diez minutos de trabajo,
   cero mantenimiento. Hoy hay 18 meses de SOL; en un año hay 30. **Solo se puede hacer
   empezando**, y el costo de no hacerlo es que dentro de un año se esté igual que hoy.
3. **El test TRANSVERSAL de prima de volatilidad.** La ρ = +0,92 mató el *pooling temporal*
   de la corrida 8, pero en un test transversal **esa misma ρ es el factor común que se
   diferencia y desaparece** — que es lo que hace el control por barra en todo el resto del
   repo. La pregunta pasa de *"¿la prima media es positiva?"* (pide 23 años) a *"¿el
   subyacente con IV/RV más alto rinde peor que sus pares?"*. Son 5-7 nombres, es flaco,
   pero es una forma funcional que no se probó y donde el ruido que mata está ausente por
   construcción.
4. **Dejar correr las tres fechas** y no tocarlas.

### Lo que NO haría

- **Barrer parámetros del detector de gráficos** para ver si aparece algo. Con 180 brazos ya
  salió una máscara aleatoria con p = 0,0495. Ese camino tiene el peaje escrito.
- **Agregar features al ranking transversal.** La §2 dice por qué: el error estándar no se
  mueve con más brazos, se mueve con más años.
- **Probar "un horizonte más largo".** Ya está medido y es un empate (corrida 13).
- **Re-medir el market making sobre el perpetuo.** Muerto por aritmética, no por falta de
  datos (`PREREGISTRO_MAKER.md`).

### Y la respuesta honesta a "¿qué pruebo si todo falla?"

Lo que sobrevivió dos veces **no predice nada**: cobra por asumir un riesgo (vender
volatilidad, y magnitud, que es su materia prima). Nada de lo que predice funcionó. Si el
objetivo es plata, la conclusión del repo ya está: **no está en este conjunto de
información**, y eso es una decisión sobre en qué negocio estar, no sobre qué feature
agregar.

Si el objetivo es saber, ya está logrado, y con más rigor del habitual: trece corridas
preregistradas, control por barra, bootstrap de bloques, FDR sobre el lote entero y
compuertas de potencia corridas **antes**. La corrida 12 lo demostró en vivo — una máscara
al azar con p < 0,05 que aguantó la compuerta de concentración. Mirando un brazo por vez,
eso era un descubrimiento.
