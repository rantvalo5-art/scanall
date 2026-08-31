# HANDOFF — las fuentes de información que el repo NO tiene

> Escrito el **2026-08-31**. Se puede abrir **en frío**: la §1 y la §2 tienen todo el
> contexto que hace falta, y no requieren haber leído los otros handoffs.
>
> **El punto de entrada general sigue siendo `HANDOFF_CUATRO.md`.** Este documento es una
> dirección específica: *¿qué se puede probar para detectar DIRECCIÓN que no exista ya en
> el repo?* La respuesta corta es **cuatro fuentes**, y de las cuatro **solo dos se pueden
> medir hoy**. Las otras dos solo se pueden **empezar a juntar**.
>
> **Antes de tocar nada, leer la §3.** Dice el filtro que mata a la mayoría de los
> candidatos antes de escribir una línea, y por qué.

---

## 0. Cómo arrancar (verificado el 2026-08-31, no de memoria)

```powershell
$env:PYTHONIOENCODING = "utf-8"
cd C:\Users\asd\Pictures\scanall
py -3.13 -u banco\ranking.py --nula            # el harness transversal: MDE primero
py -3.13 -u opciones\potencia_transversal.py   # el molde de compuerta mas nuevo
py -3.13 -u opciones\juntar_iv.py --dias 7     # el colector diario que ya existe
```

**Gotchas del entorno, todos pisados. No re-descubrirlos:**

- Siempre `py -3.13`, nunca `python`. Siempre `-u`.
- `$env:PYTHONIOENCODING = "utf-8"` o revienta con cp1252.
- Los procesos se llaman `python3.13`. **`ps` de Git Bash NO los ve** — `Get-Process
  python3.13` desde PowerShell. Un `ps` vacío no significa que murió.
- **Nunca parchear archivos con heredocs de bash.** Las tildes y los backticks fallan; un
  `\n` dentro de un heredoc se convierte en salto de línea real antes de que Python lo vea;
  y un backtick dentro de `py -3.13 -c "..."` dispara sustitución de comandos del shell y
  borra texto en silencio. **La salida: escribir un `.py` y ejecutarlo.**
- **Nunca `groupby.apply` con lambdas** sobre miles de grupos (2700× medido).
- El writer de parquet falla en esta máquina: los cachés caen a `.csv`.

**Rama:** `banco/primer-toque`. `main` corre producción (screener cada 2 min, radar cada
4h, swing, outcomes diario).

**Lo que está en vuelo al momento de escribir esto** — no romperlo:

| | |
|---|---|
| PR #26 | seguridad: redacción del token de Telegram en `screener.py`, `swing/exit_tracker.py`, `radar/radar.py` |
| PR #27 | retención de `daytrader_outcomes` de 90 → 550 días (**sin esto, dos de las tres fechas preregistradas se quedan sin datos**) |
| PR #28 | `opciones/juntar_iv.py` + workflow diario de implícita |
| corrida 10 | faltan 2,43 h de dislocación, y hay que **arrancar 18:38 UTC = 15:38 local** porque es una banda de hora del día |
| 3 fechas | radar ~8-sep y ~14-oct · fade **19-oct**. No mirarlas antes ni aflojarlas |

---

## 1. El estado del repo, en diez líneas

Este repo pasó **catorce corridas preregistradas** buscando un efecto direccional. Diez
familias estándar, nueve en cero:

> momentum transversal (4.140 hipótesis) · momentum de serie · **reversión/fade — la única
> viva** · carry/funding · order flow y microestructura · posicionamiento · régimen ·
> on-chain · estudios de evento (unlocks, listados) · patrones de velas · ML/no lineal ·
> patrones de gráfico

Lo único que sobrevivió dos veces **no es direccional**: vender volatilidad (se arbitró
antes de llegar) y **magnitud** (el radar, que sigue sin instrumento para cobrarse).

**Y el número que ordena todo lo demás** (corrida 13, MDE anualizado con el costo en la
misma unidad, 5 años y 187 pares):

| horizonte | obs. independientes | **efecto BRUTO detectable** |
|---|---|---|
| 168h (7d) | 255 | **32,4 %/año** |
| 720h (30d) | 58 | **32,6 %/año** |
| 2160h (90d) | 19 | **30,6 %/año** |

> ✔ **No hay un efecto direccional GRANDE** —del orden de 30%/año bruto— en la información
> que está en el precio, en las 200 monedas más líquidas, de 4h a 90 días.
>
> ✘ **NO está establecido que no haya uno modesto.** Un edge real de 8-15%/año **habría
> sido invisible en las catorce corridas.** Y esa pregunta **no tiene respuesta alcanzable
> con estos datos**: para llevar la resolución a 10%/año harían falta ~35 años.

**La consecuencia, que es lo que motiva este documento:** agregar una *feature* más sobre
la misma ventana de precios **no mueve el error estándar ni un poco**. Lo único que lo
podría mover es una **fuente de información distinta**.

---

## 2. Qué NO está en el repo — verificado con grep, no de memoria

Se buscó en todo el árbol (`--include=*.py --include=*.md`):

| fuente | estado | evidencia |
|---|---|---|
| **Skew de opciones** (risk reversal) | **cero** | los únicos hits de `skew` son *skew de timestamps* en `dislocacion.py`. Se midió el **nivel** de implícita (IV/RV), nunca la **inclinación** |
| **Macro / cross-asset** (DXY, Nasdaq, oro, tasas, VIX) | **cero** | los únicos hits de oro son `PAXG`/`XAUT` siendo **excluidos** del universo cripto |
| **Texto** (noticias, social, sentiment) | **cero absoluto** | ningún hit fuera de `send_telegram` |
| **Flujos de ETF** spot BTC/ETH | **cero absoluto** | ningún hit |

Todo lo demás que se mida va a ser una variante de algo ya cerrado. **Antes de proponer una
quinta fuente, correr ese mismo grep.**

---

## 3. EL FILTRO — leer antes de entusiasmarse con cualquiera

Por la §1, la pregunta correcta para una fuente candidata **no es "¿tiene información?"**.
Casi todas tienen algo. La pregunta es:

> ### ¿Es plausible que valga **más de 30%/año bruto**?

Porque menos que eso **no se puede distinguir de cero con los datos que hay**, y "no se pudo
medir" no es un resultado que justifique construir nada.

Ese filtro mata a la mayoría de entrada, y hay que decirlo en voz alta:

- **Macro**: si el DXY predijera cripto al 30%/año, sería el trade más famoso de las
  finanzas. **No.**
- **Flujos de ETF**: los publica todo el mundo con un día de retraso. **No.**
- **Sentiment general de noticias**: idem, y encima es la cosa más masticada del retail.
  **No.**
- **Skew**: **quizá, y solo en las colas** — cuando el seguro direccional se encarece de
  golpe, que es cuando puede haber algo que el precio todavía no reflejó.

**Eso no significa "no medirlas".** Significa que el resultado esperado de casi todas es
**cerrar una familia**, no encontrar un negocio — y que eso vale igual, si cuesta una tarde
y no una semana. Lo que **no** se justifica es construir infraestructura para una candidata
que ya sabemos que no puede cruzar el umbral.

### Y las dos compuertas que van SIEMPRE primero

Molde: `opciones/potencia_transversal.py` (el más nuevo y el más completo).

1. **(C) POTENCIA — antes de mirar el signo o el tamaño.** `MDE = 2,8 · σ / √n · 12`. Si
   supera el umbral preregistrado, se declara **"no se pudo medir"** y se cierra. **Y hay
   que decir cuál de las dos falló**: por *n* se reabre esperando, por *σ* no. (Corrida 9:
   266 semanas —más que la corrida 6, que sí concluyó— y no pudo, porque decidía `σ/√n`.)
2. **(P) LA PREMISA — que el mecanismo que justifica el test esté ahí.** La corrida 14 la
   estrenó y sirvió: la premisa **pasó** y la potencia falló, y eso se lee distinto de que
   fallen las dos. Dice *"la idea era buena y el dato no alcanza"*, no *"la idea era mala"*.

---

## 4. LAS CUATRO FUENTES

### 4.1 Skew de opciones — **el mejor prior, y NO se puede medir hoy**

**Qué es y por qué es distinto de todo lo demás.** Las doce familias medidas son, todas,
**patrones en precios pasados**. El risk reversal a 25 delta (IV del put menos IV del call)
no es eso: es **el precio que el mercado paga hoy por protegerse en una dirección**. Es una
clase de variable distinta —una cotización de asimetría, no un patrón inferido— y en FX y
en acciones es *la* variable canónica de posicionamiento direccional.

**El chequeo de historia, hecho el 2026-08-31. Resultado: NO HAY historia gratis.**

| puerta probada | resultado |
|---|---|
| `get_instruments(currency=BTC, kind=option, expired=true)` | HTTP 200, **56 instrumentos, un solo vencimiento** (el último). Deribit no guarda más |
| `get_tradingview_chart_data` sobre un instrumento vencido | HTTP 200, **`status: no_data`, 0 velas** |

**Pero la cadena VIVA está completa en los tres venues**, y eso es lo que habilita empezar:

| venue | endpoint | lo que trae |
|---|---|---|
| **Deribit** | `get_book_summary_by_currency` | `mark_iv` en **1026 de 1026** instrumentos, + `underlying_price`, `bid_price`, `ask_price`, `open_interest` |
| **Bybit** | `/v5/market/tickers?category=option` | `markIv`, `bid1Iv`, `ask1Iv` y **`delta`** — 770 instrumentos |
| **OKX** | `/api/v5/public/opt-summary` | `markVol`, `askVol`, `bidVol`, `delta`, `deltaBS`, `realVol`, `fwdPx` — 1426 filas |

> **`delta` viene servido por Bybit y OKX.** O sea que el risk reversal a 25 delta sale
> **directo, sin interpolar el smile** — que es la parte que normalmente ensucia esta
> medición. Esto baja el costo de construir el colector a casi nada.

**Qué hacer:** agregarlo a `opciones/juntar_iv.py`, que ya corre todos los días por
`.github/workflows/iv_diaria.yml` (PR #28) y ya commitea CSVs. **No es un proyecto nuevo:
son ~40 líneas en un cron que ya existe.**

Guardar por día y por moneda, como mínimo:

```
fecha, moneda, venue, vencimiento_dias, rr25 (iv_put25 - iv_call25),
mariposa25, iv_atm, oi_calls, oi_puts, volumen_calls, volumen_puts
```

**Cómo muere:** igual que la corrida 8 — por falta de historia. Hoy n = 0. Dentro de un año,
n = 12 meses, que es **exactamente donde la corrida 8 murió** (18 meses, MDE 39%/año). O sea
que **esto no es medible antes de ~3-4 años**, y hay que escribirlo ahora para que nadie se
ilusione en marzo.

> **Y aun así vale empezar hoy**, por la misma razón que el colector de implícita: es el
> único ítem donde **no hacer nada tiene un costo que se acumula**, y el costo de arrancar
> es una tarde.

---

### 4.2 Macro / cross-asset — **la única medible HOY, y prior bajo**

**Qué es.** DXY, Nasdaq, oro, 2y/10y, VIX como variables de **condicionamiento** del panel
cripto. Historia de décadas, gratis, en `yfinance` / FRED / Stooq.

**Por qué no está cubierto por la familia "régimen".** Aquellos eran **7 detectores internos
a cripto** (BTC sobre su EMA, breadth, vol de mercado, etc.), medidos sobre 22 trimestres, 0
pasaron. Condicionar por una variable **de afuera del sistema** es una familia distinta y
nunca se probó.

**Prior: bajo, y hay que decirlo antes de correrlo.** Que BTC correlacione con el Nasdaq es
un hecho **contemporáneo**, no predictivo. Usar el Nasdaq de ayer para el cripto de hoy es
lo primero que probaría cualquiera con acceso a los datos, que son todos.

**Por qué se corre igual:** cuesta **una tarde**, corre sobre `banco/ranking.py` sin
construir infraestructura, y **si da cero cerrás la familia 13 gratis**. El valor esperado
está en el cierre, no en el hallazgo. Eso es honesto y es suficiente.

**Cómo hacerlo, sin reinventar el harness:**

1. Preregistro primero (`banco/PREREGISTRO_MACRO.md`), con la dirección declarada.
2. Bajar las series diarias y alinearlas al panel — **cuidado con el desfase de husos y con
   los feriados de mercados tradicionales**: cripto opera 24/7 y las acciones no. Un valor
   de macro con `NaN` el fin de semana que se rellena hacia adelante es información de
   **ayer**, y está bien; rellenado hacia atrás es **lookahead**.
3. Correr la compuerta de potencia **antes** de mirar el signo.
4. Control por barra, bootstrap de bloques por semana, FDR sobre el lote entero **con los
   controles adentro**.

**Cómo muere:** por efecto, no por potencia. Hay historia de sobra, así que si da cero, el
cero es informativo y la familia queda cerrada de verdad.

---

### 4.3 Flujos de ETF spot (BTC/ETH) — **medible, casi seguro muere en la compuerta**

**Qué es.** Creaciones y redenciones diarias de los ETF spot. No es un patrón inferido: son
**compras y ventas reales**. Públicos y scrapeables (Farside Investors publica la tabla
diaria), con historia desde **enero 2024**.

**El problema, dicho antes de bajar un dato:** son **dos nombres** y **~20 meses**, o sea
~90 semanas independientes. La corrida 9 tuvo **266 semanas** y **no pudo concluir**.

**Qué hacer:** correr **solo la compuerta de potencia**, sobre los datos ya publicados,
antes de construir nada. Es media tarde y decide. Si el MDE no cruza, se anota "no se pudo
medir, falló por n" y se cierra — y esa es la respuesta correcta, no un fracaso.

**El riesgo de método específico de esta fuente:** las tablas de flujos **se revisan hacia
atrás**. Un CSV bajado hoy no es lo que se veía ese día. Si no se puede reconstruir la
versión point-in-time, **hay que decir que el backtest tiene look-ahead de revisión** y
tratar el resultado como un techo, no como una estimación.

---

### 4.4 Texto / noticias con LLM — **el hueco más grande, el peor dato**

**Qué es y por qué es la categoría más interesante.** Es la única clase de información donde
el repo tiene **cero**. Todo lo medido es precio, volumen, derivados y on-chain. Y hay algo
genuinamente nuevo: **un LLM puede puntuar un titular por dirección de forma barata y
consistente**, cosa que no se podía cuando se escribió casi toda la literatura de sentiment.

**El problema es fatal y es de datos, no de método: necesitás historia point-in-time.**

- CryptoPanic y similares dan ~30 días hacia atrás en el tier gratis.
- GDELT tiene historia enorme y gratis, pero es de noticias generales, no cripto-específica.
- La API de X dejó de ser viable por precio.
- Y cualquier dataset que bajes hoy tiene **sesgo de supervivencia** (las notas borradas no
  están) **y revisiones**.

**Qué hacer, si se hace:** un cron que guarde titulares crudos todos los días —**crudos, con
timestamp de ingesta, sin puntuar**— y el puntaje del LLM se calcula después. Guardar el
texto y no el score es lo que permite cambiar de modelo sin perder la historia, y lo que
evita que un cambio de prompt reescriba el pasado.

**Prior por el filtro de la §3: no cruza los 30%/año.** Si se hace, es por completitud del
mapa, no porque se espere un negocio.

---

## 5. EL ORDEN DE TRABAJO

### Ahora (una tarde cada una)

1. **Macro sobre `ranking.py`.** Es la única de las cuatro que se puede **medir** hoy con n
   real. Preregistro → compuerta de potencia → medición. Resultado esperado: cero, y una
   familia cerrada por el precio de una tarde.
2. **Compuerta de potencia de los flujos de ETF.** Media tarde, sin construir nada, y decide
   si vale seguir. Casi seguro dice que no.

### Ahora también, pero es plomería, no medición

3. **Agregar el skew a `opciones/juntar_iv.py`.** ~40 líneas sobre un cron que ya existe.
   `delta` viene servido por Bybit y OKX, así que el RR25 sale sin interpolar. **No se puede
   medir con esto durante años — se está comprando opcionalidad, no probando una hipótesis.**
4. **Si se quiere el de texto: el mismo cron, titulares crudos.** El costo marginal de sumar
   una fuente más al workflow diario es casi cero.

> **Si vas a juntar, juntá las tres en el mismo cron.** Ya hay un workflow diario que
> commitea CSVs (`iv_diaria.yml`). Sumarle skew y titulares no agrega mantenimiento.

### Nunca

5. **No construir un detector, un backtest ni una estrategia sobre ninguna de estas fuentes
   antes de que su compuerta de potencia pase.** Es la regla que ahorró las corridas 8, 9,
   13 y 14 enteras.

---

## 6. Las reglas que no se negocian

Son del repo, no de este documento, y ya se cobraron resultados.

> **La regla de parada se escribe ANTES de mirar.** Si se afloja después de ver un número,
> el experimento no vale.

> **La dirección de una hipótesis se declara antes de medirla.** Medido dos veces: 3 de los
> 5 mejores brazos de la corrida 7 estaban invertidos, y el brazo más tentador de la
> corrida 12 también.

> **El p que decide es el de BLOQUES, no el binomial. El n efectivo son las SEMANAS.** Con
> 180 brazos, una **máscara al azar** dio **p = 0,0495** y aguantó `sin_top3`.

> **El FDR va sobre el LOTE ENTERO, con los controles adentro.**

> **Un control tiene que poder GANAR, o no es un control.** Una ruptura pelada le ganó a las
> cinco figuras de gráfico.

> **Todo MDE se reporta ANUALIZADO**, y el **costo en las mismas unidades que el ruido**,
> calculado **antes** de elegir la resolución.

> **Contar el n NO alcanza: hay que medir la σ, y decir cuál de las dos falló.**

> **Una cartera long-short no hereda la σ PROMEDIO del universo: hereda la de los EXTREMOS.**
> La corrida 14 predijo 10,8%/año con `σ·√(2(1−ρ))` y midió 28,4, porque esa cuenta supone
> σ iguales entre nombres y no lo son.

> **Un número que contradice una estructura de mercado conocida es un bug hasta que se
> demuestre lo contrario.** Delató un error de unidades de 100× en OKX y un filtro mal
> puesto en `exchangeInfo`.

> **Y una que este documento agrega:** *un experimento preregistrado a meses o años vista
> depende de que el dato siga existiendo ese día.* Antes de escribir una fecha, mirar qué la
> borra: retención, cuota, un cron que se apaga solo. Ya pasó — `daytrader_outcomes` se
> purgaba a los 90 días y dejaba sin datos a dos de los tres chequeos del fade.

---

## 7. Lo que este plan NO autoriza

- **Agregar features al ranking transversal.** La §1 dice por qué: el error estándar se
  mueve con más años, no con más brazos.
- **Barrer parámetros** de ninguna de estas fuentes "a ver si aparece algo".
- **Probar un horizonte más largo.** Medido (corrida 13): es un empate. El costo cae 13× y
  la precisión cae lo mismo.
- **Reabrir vender volatilidad, cash-and-carry, market making sobre el perpetuo, ni patrones
  de gráfico.** Están cerrados con el motivo escrito.
- **Tocar las tres fechas preregistradas** ni mirar sus números antes de tiempo.

---

## 8. La expectativa honesta

De las cuatro, **tres no cruzan el filtro de la §3** y la cuarta —el skew— **no es medible
durante años**. El resultado más probable de ejecutar todo este plan es **dos familias más
cerradas y dos colectores corriendo**.

Eso no es un fracaso del plan: es lo que el plan dice de antemano que va a pasar, y por eso
ninguna de las tareas cuesta más de una tarde. **Si algo acá va a valer, va a valer dentro de
tres años y va a ser el skew** — y la única forma de que exista esa opción es que el cron
empiece hoy.

Y lo que ya está dicho en `HANDOFF_CUATRO.md` §6 sigue en pie: si el objetivo es plata, la
conclusión del repo ya está —**no está en este conjunto de información**— y eso es una
decisión sobre en qué negocio estar, no sobre qué fuente agregar.
