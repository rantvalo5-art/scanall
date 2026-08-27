# radar/ — qué monedas se van a MOVER en las próximas 4h. No para dónde.

```
py -3.13 -u radar.py                       # top-8
py -3.13 -u radar.py --k 15                # más nombres
py -3.13 -u radar.py --json                # para pegarlo a otra cosa
py -3.13 -u radar.py --telegram            # además lo manda
py -3.13 -u radar.py --min-atr 0.005       # piso de volatilidad (NO MEDIDO, ver abajo)
```

## Qué hace

Cada corrida: baja velas de 1h de ~200 pares, calcula para cada uno cuántas operaciones
tuvo la última hora contra su mediana de los últimos 7 días (`n_surge`), **ordena las
monedas entre sí en ese mismo instante**, y te devuelve las k primeras.

Eso es todo. **240 líneas y cero archivos de configuración.**

## Por qué es tan chico comparado con `../screener.py`

El screener de la raíz tiene **2.111 líneas y 233 parámetros**. Se midió, y:

| qué se midió | resultado | dónde |
|---|---|---|
| ¿cuándo avisa? | el precio ya subió **+3,12 ATR** antes de la alerta, y baja −0,94 después | `banco/PREREGISTRO_RANKING.md` §9 |
| ¿el score de 15 puntos ordena? | **no** — cae dentro del margen de error de tres máscaras al azar; 0 de 735 combinaciones sobreviven | idem |
| ¿elegir bien la moneda aporta? | **+0,012 ATR ≈ 0.** El 60% de la pérdida es el MOMENTO de entrada | idem |
| ¿se puede predecir la dirección? | **0 de 4.140 brazos** — precio, flujo de órdenes y posicionamiento de futuros, las dos direcciones, de 4h a 7d, 5 años, 4 regímenes | `banco/PREREGISTRO_TRANSVERSAL.md` corrida 4 |
| ¿se puede predecir la magnitud? | **sí**, 38 rankings sobreviven y aguantan los cuatro regímenes | idem, corrida 3 |

Este archivo hace **solo lo que sobrevivió**.

## Lo que promete, con los números

Medido sobre 46 pares, 2021-10 → 2026-07, **251 semanas**, top-8 contra el universo de la
misma barra:

| | |
|---|---|
| spread de `n_surge` | **+0,511 ATR base**, **100% de las 251 semanas**, t = 22,6 |
| recorrido en 4h de las elegidas | **2,85%** contra **2,37%** del universo → **1,21×** |
| la elegida supera la mediana de su barra | **62,6%** de las veces (línea base 49,5%) |

**Es modesto y hay que decirlo así:** *"se mueve ~21% más que la típica"*, no *"se mueve
el doble"*. Lo que lo hace valioso no es el tamaño sino la **consistencia**: 97% de las
semanas, y con el mismo signo en bear 2022, bull 2023-24, lateral 2024-25 y bear 2025-26,
mientras un ranking al azar se queda en cero.

## Decisiones de diseño, y por qué

**Una sola feature, no una combinación.** `banco/combo.py` midió que combinar
`oi_rel_168 + n_surge + turnover` gana **+0,0077** contra un MDE de **±0,078**: ruido.
Tres features correlacionadas no son tres features.

**`n_surge` y no `oi_rel_168`,** aunque el segundo mide 0,10 mejor (dentro del MDE):
`n_surge` sale de los mismos klines, no necesita que el par tenga perpetuo (~20% no lo
tiene) ni una request extra por símbolo. `oi_rel_168` se muestra al lado, informativo.

**Horizonte de 4h, y se midió — no se eligió.** La primera versión usaba 24h por
inercia: fue el horizonte que el banco fijó al principio y nunca se cuestionó.
`banco/horizonte_util.py` lo barrió con métricas **sin escala**, que son las únicas
comparables entre horizontes (el spread crudo crece con el tiempo, y la consistencia
semanal sube a horizontes cortos solo porque hay más barras por semana):

| horizonte | múltiplo | tasa | t |
|---|---|---|---|
| **4h** | **1,21×** | **62,6%** | **22,6** |
| 8h | 1,18× | 62,2% | 19,6 |
| 24h | 1,15× | 61,3% | 15,5 |
| 72h | 1,13× | 59,2% | 9,9 |
| 7d | 1,11× | 59,6% | 7,0 |

Monótono: la señal **se decae con el tiempo**. Es agrupamiento de volatilidad — la
información de un pico de actividad dura horas, no días.

**Esto ubica al radar en el horizonte del day trader, no en el del swing.** No es
ninguno de los dos: el day trader escanea cada 2 min y sostiene horas; el swing usa
1h/4h/1d y sostiene ~7 días. El radar rebalancea cada 4h. Es un tercer horizonte, y es
el que la medición eligió.

**Ranking transversal, no umbral.** La posición de cada moneda es contra las otras **de
este mismo instante**. Un umbral fijo mezcla "qué moneda es" con "qué hora es"; el rank
dentro de la barra separa las dos.

**Se descarta la vela en curso.** Usarla es mirar el futuro a medias, y además haría que
el ranking cambiara según el minuto en que corrés el script.

## Lo que NO hace, a propósito

- No dice comprar ni vender. **No hay dirección medida. No la hay.**
- No puntúa de 0 a 15 ni tiene buckets BEST/STRONG/WATCH.
- No tiene `config.json`. La única perilla es `--min-atr`, **apagada por default y no
  medida**: el ranking es relativo, así que una moneda quieta que se activa sigue siendo
  quieta (SUNUSDT entra con 4,8× y recorre 1,1%). Si querés movimiento absoluto ayuda,
  pero te saca de lo medido.

## Lo honesto sobre qué hacer con esto

Esto es un **radar**, no una máquina de ganar plata. Te dice dónde va a pasar algo. Qué
hacer con eso no está resuelto: para cobrar movimiento sin saber la dirección hace falta
un instrumento convexo (opciones), y en cripto eso prácticamente solo existe para BTC y
ETH. Sintetizarlo con órdenes stop ya se midió y **no funciona** (regalás k·ATR por
trade).

Sesgos declarados: el universo es el ranking de volumen de **hoy** (los deslistados no
están, sesga hacia mejor), y la calibración se midió sobre 46 pares grandes con perpetuo
desde 2021 — **no se puede extrapolar a la cola ilíquida**, donde además los costos
reales son 1,5× a 6,3× lo que asume el banco (`banco/libro.py`).

---

## Correrlo solo, y medirlo

### 1. Crear la tabla

Pegá `tabla.sql` en el SQL Editor de Supabase. Crea `radar_runs` con su índice único
(para que un cron disparado dos veces no duplique) y las políticas de RLS que hacen falta
para escribir con la anon key.

### 2. El cron

`.github/workflows/radar.yml` corre **cada 4 horas** (00:10, 04:10, … UTC) y guarda el
universo entero más manda el top-8 por Telegram. Usa los secrets que ya tenés configurados:
`SUPABASE_KEY`, `DAY_TELEGRAM_TOKEN`, `DAY_TELEGRAM_CHAT_ID`.

**La cadencia tiene que ser igual al horizonte.** Con `paso = horizonte = 4h` las barras
no se solapan y el n contado es el n real. Correrlo cada hora con horizonte de 4h daría
corridas solapadas, y el forward test heredaría exactamente el defecto que el resto del
repo arrastra por contar entradas solapadas como si fueran independientes.

Para mirarlo cuando quieras, corrélo a mano — sin `--supabase` no ensucia nada.

### 3. Medirlo

```
py -3.13 -u medir.py
```

Lee lo guardado, reconstruye con velas posteriores lo que **efectivamente** pasó, y lo
compara contra los números preregistrados:

| | medido antes | en vivo |
|---|---|---|
| spread | +0,511 ATR base | ? |
| múltiplo de camino | 1,21× | ? |
| tasa de acierto | 62,6% | ? |

**La regla de parada está escrita en `medir.py` antes de que existan datos:**

- Con menos de **8 semanas** no concluye nada. La unidad independiente es la semana, no
  la corrida: 6 corridas por día durante 10 días no son 60 datos.
- Si a las 8 semanas el spread es **≤ 0**, no replicó y el radar se apaga.
- Entre 0 y +0,5 se reporta como **réplica débil** y sigue vivo: la primera medición de
  cualquier cosa exagera, porque se encontró mirando, y lo que se encuentra mirando es la
  parte alta del ruido.
- **No se toca `n_surge` ni `k` por lo que salga acá.** Ajustar el screener con el
  resultado del forward test convierte el out-of-sample en in-sample, y después no queda
  ninguna ventana limpia para volver a preguntar.
