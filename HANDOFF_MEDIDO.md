# HANDOFF — los tres sistemas, medidos por primera vez

> Escrito el **2026-09-02**. Se puede abrir **en frío**: las §0, §1 y §2 tienen todo el
> contexto necesario.
>
> **El punto de entrada general sigue siendo `HANDOFF_CUATRO.md`**, y la dirección de
> fuentes nuevas es `HANDOFF_FUENTES_NUEVAS.md`. Este documento cubre otra cosa: lo que
> pasó cuando se dejó de investigar y se midió **lo que ya estaba corriendo en producción**.
>
> **Si vas a hacer una sola cosa, andá a la §3.** Lo demás ya se resuelve solo.

---

## 0. Cómo arrancar (verificado el 2026-09-02, no de memoria)

```powershell
$env:PYTHONIOENCODING = "utf-8"
cd C:\Users\asd\Pictures\scanall

$env:SUPABASE_KEY = "..."                       # la clave NO esta en el repo
py -3.13 dump_outcomes.py                       # daytrader -> outcomes_dump.json
py -3.13 dump_outcomes.py --tabla screener_outcomes   # swing -> screener_outcomes_dump.json

py -3.13 -u dt_vivo.py --sistema daytrader
py -3.13 -u dt_vivo.py --sistema swing
```

**Gotchas del entorno, todos pisados. No re-descubrirlos:**

- Siempre `py -3.13`, nunca `python`. Siempre `-u`.
- `$env:PYTHONIOENCODING = "utf-8"` o revienta con cp1252.
- **Nunca parchear archivos con heredocs de bash.** Falló otra vez en esta sesión (tildes y
  `\n`). La salida: escribir un `.py` y ejecutarlo, o usar la herramienta de edición.
- **Nunca editar un `.json` de config con `json.load`/`json.dump`.** El round-trip normaliza
  `0.30`→`0.3` y expande arrays, y te deja un diff de 14 líneas en secciones de producción
  que no tocaste. Editar la línea con `sed`.
- No hay `lxml` ni `html5lib` ni `bs4`: `pandas.read_html` revienta. Para HTML, `html.parser`
  de la stdlib.
- No hay `yfinance` ni `pandas_datareader`. FRED da timeout; Stooq devuelve un desafío de
  JavaScript. **Yahoo Finance (`query1.finance.yahoo.com/v8/finance/chart/`) sí anda.**

**Rama:** el trabajo del banco vive en `banco/primer-toque`. `main` corre producción.

---

## 1. Lo que cambió: los tres sistemas tienen un número

Hasta esta sesión el repo tenía 14 corridas de investigación y **cero mediciones de lo que
estaba corriendo en producción**. Ahora:

### El banco — cerrado, y con dos corridas más

| corrida | fuente | resultado |
|---|---|---|
| **15** | macro / cross-asset (NDX, DXY, oro, 10a, VIX) | se **midió**: 0 de 120 brazos. Cierre **por efecto**, acotado a **35,5 %/año bruto** |
| **16** | flujos de ETF spot BTC/ETH | **no se pudo medir**. Cierre **por potencia**, falla por **σ**. El diseño más favorable queda en 36,4 %/año contra un umbral de 10 |

Preregistros en `banco/PREREGISTRO_MACRO.md` y `banco/PREREGISTRO_ETF.md`, con los resultados
debajo de la línea.

> **Corrección a `HANDOFF_FUENTES_NUEVAS.md` §4.2**, que decía que macro *"muere por efecto,
> no por potencia, porque hay historia de sobra"*. **La historia que ata no es la del macro
> —que tiene décadas— sino la del panel cripto**, y el MDE de `ranking.py` sale de la nula,
> que no sabe con qué score se rankea. Un brazo macro hereda **exactamente** el MDE de la
> corrida 13. El cero es informativo, sí, pero **a 36 %/año**, no "de verdad".

### El daytrader — pierde, y es medible

`py -3.13 -u dt_vivo.py --sistema daytrader`. **1.526 entradas deduplicadas en 11 semanas**
(no las 14.736 filas de la tabla: FADING es una **salida** y RIDING/HOLD repiten sobre la
misma posición). Neto de 0,20 %:

| horizonte | media semanal | semanas > 0 | MDE | |
|---|---|---|---|---|
| 15m | −0,07 % | 27 % | 0,32 | dentro del MDE |
| 1h | −0,49 % | 18 % | 0,49 | dentro del MDE |
| **4h** | **−1,14 %** | **0 %** | 0,59 | **negativo, ~2× el MDE** |
| 24h | −1,73 % | 10 % | 1,31 | negativo, supera el MDE |

**Y el score no ordena — está invertido:** ρ = −0,035 (p = 0,90), con BEST −1,40 %,
STRONG −1,04 %, WATCH −0,19 %. MFE +3,31 % contra MAE −3,62 %: **asimetría 0,91**. La alerta
agarra volatilidad, no dirección.

### El swing — plano

`py -3.13 -u dt_vivo.py --sistema swing`. **1.924 entradas en 14 semanas.** Los cuatro
horizontes (+0,03 / +0,07 / −0,05 / −0,25) caen **dentro del MDE**: no pierde como el
daytrader, no se lo puede medir. ρ = +0,0009 — el score no ordena, pero tampoco está
invertido.

> **Asimetría MFE/|MAE| = 1,12** (favorable), contra 0,91 del daytrader. Es el único número
> de toda la sesión que apunta para arriba.

**Ojo:** hasta ahora se lo medía con los horizontes del **daytrader** (hasta 24h). Su tesis
es 1h/4h/1d/1w. Los datos a 7d **sí existen** (§3.2).

---

## 2. Las cuatro fechas — todo esto se resuelve solo

**No hay nada que construir para que ocurran.** Los crons están sanos (verificado el
2026-09-02: `screener` cada 2 min, `swing` cada hora, `outcomes` diario 04:00 UTC,
`backfill_7d` cada ~8 h, `exit_tracker` y `radar` corriendo).

| fecha | qué se resuelve | preregistro | estado |
|---|---|---|---|
| ~8-sep y ~14-oct | radar / magnitud | (anterior) | — |
| **19-oct** | el **fade** — la única familia direccional viva | (anterior) | retención 550 d ✅ |
| **21-oct** | el **swing a 7d** | `swing/PREREGISTRO_7D.md` | datos existentes ✅ |
| **24-nov** | el **timing de salida** del daytrader | `PREREGISTRO_SALIDA.md` | instrumentado y confirmado ✅ |

**La regla del repo aplica a las cuatro: no se miran antes, ni se aflojan después.**

Y la que este handoff agrega, porque casi se pierde: **una fecha preregistrada depende de que
el dato siga existiendo ese día.** La retención de `daytrader_outcomes` pasó de 90 a 550 días
(PR #27) y la de `screener_outcomes` también (PR #33) — sin eso, dos de las cuatro se
quedaban sin datos en silencio.

---

## 3. Lo que queda por hacer

### 3.1 — Confirmar que el timing de salida está entrando (5 minutos, ALTA)

Es lo único **no verificado end-to-end**. El log del 2026-09-01 04:00 no emitió el aviso de
columnas faltantes, así que el `ALTER TABLE` tomó — pero **nadie miró las filas todavía**:

```powershell
py -3.13 dump_outcomes.py
py -3.13 -c "import json,pandas as pd; D=pd.DataFrame(json.load(open('outcomes_dump.json',encoding='utf-8'))); print(D[['mfe_min_4h','mae_min_4h']].notna().sum())"
```

Si dan 0, el reloj del 24-nov **no arrancó** y hay que ver por qué. Si dan >0, listo y no se
toca hasta noviembre.

### 3.2 — Nada, para el swing a 7d

Los datos **ya existen**: `swing/backfill_7d.py` los junta desde antes de esta sesión, con
cron propio cada 6 h. Al 2026-09-01 había 3.278 de 3.483 filas (94,1 %) con `price_7d`,
del 3-jun al 24-ago.

**Lo que falta es tiempo, no código.** El bloque es de **14 días** (el doble del horizonte,
porque a 7d los bloques semanales se solapan y el bootstrap regala significancia), y hoy son
82 días = **5,9 bloques** contra los 10 que pide el preregistro. De ahí sale el 21-oct.

### 3.3 — Los PR viejos sin mergear (MEDIA)

| PR | qué |
|---|---|
| **#26** | **seguridad**: tapar el token de Telegram en tres sitios. Es el único con riesgo real |
| **#28** | `opciones/juntar_iv.py` + workflow diario de implícita |
| #15, #16, #17, #18 | day trader / backtest, de antes. Revisar si siguen teniendo sentido |

**#26 primero.** Los otros están sin tocar hace tiempo y conviene decidir si se cierran.

### 3.4 — El colector de skew (BAJA en urgencia, ALTA en costo de no hacerlo)

`HANDOFF_FUENTES_NUEVAS.md` §5.3: ~40 líneas sobre el cron de PR #28. **No se puede medir
durante años** — se compra opcionalidad, no se prueba una hipótesis. Pero es el **único ítem
donde no hacer nada tiene un costo que se acumula**: cada día sin colectar es un día que
nunca vas a tener.

### 3.5 — El radar sigue sin instrumento

Magnitud es lo único que sobrevivió dos veces en todo el repo, y sigue sin forma de cobrarse.
**Eso es un problema de ejecución, no de investigación**, y ninguna corrida más lo va a
resolver.

### 3.6 — Lo que NO hay que hacer, con nombre

**No invertir el score del daytrader.** La cuenta es tentadora: −1,14 % neto a 4h es −0,94 %
bruto, así que shortear las mismas alertas daría +0,74 % neto, por encima del MDE de 0,59.
No:

- **Eso ya es el fade**, la única familia direccional viva, y **ya tiene fecha: 19-oct**.
  Derivarlo ahora de datos ya mirados es fabricar una versión post-hoc de un test que está
  corriendo bien.
- ρ = −0,035 con p = 0,90: el score **no ordena**, ni al derecho ni al revés. Tres buckets
  sobre 11 semanas no son una monotonía.
- El repo ya se comió esa tentación **tres veces**: corrida 7 (3 de los 5 mejores
  invertidos), corrida 12 (el mejor de todos), corrida 15 (el mejor brazo macro estaba en la
  dirección exploratoria y murió en el FDR).

---

## 4. Los errores de esta sesión, que son reglas

Todos son de la misma familia —**inferir en vez de verificar**— y los tres costaron trabajo
real. Van acá porque se repiten.

> ### **Un archivo del working tree NO es el estado de la base.**
> Se leyó `outcomes_dump.json` como si fuera la tabla y se concluyó que la retención había
> borrado el 92 % del registro. El archivo era un volcado viejo y parcial: la tabla tenía
> 14.736 filas y 68 días. Se perdió media sesión "recuperando" datos que nunca se perdieron.
> **Antes de diagnosticar sobre un dump, mirar su fecha y volver a volcarlo.**

> ### **Antes de construir, `grep` por lo que ya existe.**
> Se agregó un llenador de 7d a `swing/update_outcomes.py` porque las columnas "estaban
> vacías". **`swing/backfill_7d.py` ya existía**, con cron propio, y su docstring ya
> explicaba el diseño entero. Estaba a la vista en el primer `ls`. Resultado: dos jobs
> compitiendo por las mismas filas, y un PR entero que hubo que revertir (#34).

> ### **Lo que vale para una tabla no vale para la otra.**
> Ese error salió de mirar los nulos del dump del **daytrader** —donde las columnas de 7d sí
> están vacías— y generalizarlo al **swing**, donde estaban llenas al 94 %.

> ### **`py_compile` no prueba que el módulo ande.**
> Al revertir se borraron `get_klines_range` y `price_at`, que habían quedado entre las dos
> funciones que se sacaban. **Compilaba igual**, porque Python resuelve esos nombres recién
> al llamarlos. Habría reventado en la primera corrida. **Para un revert, `git checkout
> <commit> -- <archivo>`, no recortar a mano** — y verificar importando el módulo, no
> compilándolo.

> ### **Un `except` que traga es peor que un crash.**
> `patch_outcome` se come las excepciones e imprime. PostgREST rechaza el PATCH **entero**
> con 400 si falta una columna: sin el `ALTER TABLE`, **todas** las filas habrían dejado de
> actualizarse **con el workflow en verde**. Ahora degrada solo: reintenta sin las claves
> nuevas y avisa una vez. **Un paso con `continue-on-error: true` necesita que alguien lea
> el log, no el estado del run.**

> ### **El n efectivo no son las filas.**
> 14.736 filas del daytrader son **1.526 trades y 11 semanas**. FADING es una salida, RIDING
> "se repite cada run". **La taxonomía de las señales es la mitad del trabajo**, y sin ella
> cualquier tabla de win rate miente.

---

## 5. Lo que este handoff NO autoriza

- **Mirar cualquiera de las cuatro fechas antes de tiempo**, ni aflojar sus umbrales.
- **Invertir el score del daytrader** (§3.6).
- **Agregar horizontes** al swing más allá de 7d, ni al daytrader más allá de 4h. Los dos
  preregistros fijan uno y solo uno.
- **Reabrir el banco.** Las 16 corridas están cerradas con el motivo escrito, y la §1 dice a
  qué resolución.
- **Tocar `screener_pairs_snapshot`.** Es una **vista**, no una tabla; un DELETE falla con
  `55000`. Y no hay problema de espacio: la base entera son ~45 MB de los 500 del free tier,
  con `screener_runs` (36 MB) acotada por su purge de 30 días.

---

## 6. La pregunta de fondo, que sigue abierta y no es técnica

Esta sesión empezó con *"estoy seriamente considerando volver a comenzar otro proyecto como
este, porque ya ni sé qué pasa con el daytrader ni cómo mejorar, o con el swing"*.

Ahora se sabe: **el banco está cerrado, el daytrader pierde de forma medible, y el swing es
plano.** Eso no decide nada por sí solo, pero era la información que faltaba para decidir.

Y lo que ya decía `HANDOFF_CUATRO.md` §6 sigue en pie: si el objetivo es plata, la conclusión
del repo ya está —**no está en este conjunto de información**— y eso es una decisión sobre en
qué negocio estar, no sobre qué feature agregar.

**Un proyecto nuevo con la misma forma choca con la misma pared**, y ahora se sabe que
enterarse cuesta ~16 corridas. Lo que cambia la forma es otra información (el skew, que pide
años de colectar), otro mercado, o un negocio que no sea alpha.
