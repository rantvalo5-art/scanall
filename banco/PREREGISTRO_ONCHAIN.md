# PREREGISTRO — CORRIDA 6: ON-CHAIN, la información que no sale del precio

> Escrito el **2026-08-28**, después de medir el n y el MDE (que es lo que el handoff
> manda hacer **primero**) y **antes de correr un solo brazo real**. Los resultados van
> debajo de la línea, como en las corridas 1 a 5.
>
> Es la dirección **4.3** de `HANDOFF_SIGUIENTE.md`.

---

## 1. Por qué esto no es más de lo mismo

Las corridas 1 a 5 midieron **4.140 + 424 + 592 hipótesis** sobre tres fuentes: precio,
flujo del kline, y posicionamiento de futuros. Las tres tienen algo en común: **salen del
mercado**. On-chain es la única clase de información que el repo nunca tocó y que se
genera en otro lado — en la cadena, por gente moviendo monedas.

Mecanismo plausible y declarado de antemano: **actividad de cadena creciente = adopción o
distribución real**, y **emisión nueva = presión de oferta que no depende de nadie**.

## 2. LA FUENTE, y lo primero que hay que decir es lo que NO tiene

**CoinMetrics Community API** — gratis, sin API key, diaria, desde el génesis de cada
cadena. Es la única fuente gratis con cobertura multi-activo real.

> **Los flujos de exchange —la idea titular de 4.3— NO se pueden medir.**
> `FlowInExNtv` / `FlowOutExNtv` existen en el tier gratis para **2 activos** de 6.093.
> Así que *"monedas saliendo de exchanges = menos oferta vendedora"* **queda sin medir**,
> y esta corrida **no la cierra**. Lo que sigue mide otra cosa: actividad, tenedores y
> emisión.

Lo que sí hay, para los 48 activos que además cotizan en Binance:

| métrica | qué es | ¿sale del precio? |
|---|---|---|
| `AdrActCnt` | direcciones activas por día | no |
| `AdrBalCnt` | direcciones con saldo > 0 (tenedores) | no |
| `TxCnt` | transacciones por día | no |
| `TxTfrCnt` | transferencias por día | no |
| `IssTotNtv` | emisión nueva del día | no |
| `SplyCur` | oferta circulante | no |
| `CapMVRVCur` | market cap / realized cap | **sí, en parte** — ver §5.5 |

Se **excluyen a propósito** `PriceUSD`, `ReferenceRate*`, `ROI*`, `CapMrktCurUSD/EstUSD`
(precio × oferta) y el volumen reportado: meterlas sería rankear precio con otro nombre.

## 3. El lookahead — el riesgo serio de esta corrida, y cómo se elimina

Una métrica diaria del día D cubre 00:00-23:59 de D y **no existe hasta que D termina**.
Usarla para entrar durante D es mirar el futuro, y es un error que no falla ruidosamente:
simplemente infla todo.

CoinMetrics publica `AssetEODCompletionTime`, el instante en que el dato del día queda
firme. **Medido sobre la muestra, no supuesto:**

| demora de publicación (desde que empieza el día que describe) | |
|---|---|
| mediana | **24,9 h** |
| p95 | **29,0 h** |
| máximo | **61,3 h** |

O sea que **un lag fijo de un día habría metido lookahead en la cola** (el máximo son 2,5
días). Por eso `onchain.alinear()` **no usa lag**: para una entrada al cierre de la barra
`t` toma la última fila diaria con `completo <= t + 1h` — *el dato que realmente existía
en ese instante*. El `completo` se pasa por un máximo acumulado, así que un día que salga
desordenado no puede adelantar a otro.

## 4. n POST-JOIN y MDE — contados ANTES de estimar nada

Es la regla del handoff, y es la que convirtió un "no se pudo medir" en un "no está" en
unlocks y en la cola ilíquida. **Ya está corrida** (`correr_onchain.py --nula`):

| | |
|---|---|
| filas del tablero | 75.709 |
| **filas con on-chain** | **70.615 (93,3%)** |
| barras | 1.795 |
| activos | **41** |
| **semanas (el n independiente)** | **257** |
| activos por barra | mediana 39, p5 38 — **100% de las barras** con ≥ 30 |

**MDE con la nula real** (8 rankings al azar × 3 objetivos, top-k=8):

| objetivo | MDE |
|---|---|
| largo | **±0,0647 ATR** |
| corto | **±0,0645 ATR** |
| magnitud | ±0,0847 ATR |

> **La compuerta de viabilidad PASA, y con holgura.** ±0,065 es prácticamente el MDE de la
> corrida 3 (±0,062, con 46 pares y 251 semanas). **Esto NO es la trampa de unlocks**: no
> es un n chico con el que no se puede concluir, es exactamente la misma potencia con la
> que se cerró la familia de derivados.

## 5. Diseño — espejo de la corrida 3

Lo único que cambia respecto de la corrida 3 es **la fuente**. Todo lo demás queda igual
para que el comparador exista: ventana **2021-08-01 → 2026-08-01**, velas de 1h,
`paso = horizonte = 24h` (sin solape), **`k = 8`**, `MIN_SYMS = 30`, las dos direcciones
de cada score, los tres objetivos, y las seis compuertas cableadas de `ranking.py` con
FDR q=0,10 **sobre el lote entero**.

### 5.1 El universo: 43, y se filtró por CLASE DE ACTIVO antes de correr

138 activos tienen on-chain diario; **48 cotizan en Binance** y los 48 tienen historia
desde 2021-08 o antes. **Se sacan 5 que no son cripto** —`USDC`, `TUSD` (stablecoins),
`PAXG`, `XAUT` (oro), `WBTC` (es BTC envuelto, duplicaría la sección cruzada)— aplicando
la regla de método que dejó la corrida 5. **Se sacan ANTES de correr, no después de ver un
resultado.** Quedan 43, de los que 41 sobreviven al warmup.

### 5.2 Cada métrica entra en cinco formas

Nivel crudo, cambio a 7d, cambio a 30d, z contra su propia historia de un año, y
**percentil** contra su propia historia. Todas mirando solo al pasado.

El percentil y el z son **los comparables entre cadenas**. El nivel crudo rankea *qué
cadena es* —bitcoin siempre tiene más direcciones activas que decred— y eso ya mató a un
candidato en la corrida 4 (`tt_pos` funcionaba en nivel crudo y no en `tt_pos_pct`). Ver
la regla 6.2.

### 5.3 Los brazos de precio y flujo van en el MISMO lote

Igual que en la corrida 3. Sirven de comparador interno y **pagan su multiplicidad**: el
FDR corre sobre todo junto, no una familia por vez.

### 5.4 Costos

0,20% (el del banco) y **0,50%**. La corrida 5 dejó la lección: un sobreviviente que solo
vive al costo barato no cuenta.

### 5.5 `CapMVRVCur` es híbrida y se trata como tal

MVRV = capitalización de mercado / capitalización realizada. **El numerador es precio.**
Va en el lote porque el denominador (el costo base on-chain de las monedas) es información
real que no está en la vela, pero **si el único sobreviviente es MVRV, hay que mostrar que
hace algo que los brazos de precio del mismo panel no hacen** (regla 6.4).

## 6. Regla de parada — fijada ANTES

1. **Si ningún brazo direccional sobrevive**, queda cerrada **esta celda**: ranking
   transversal, top-k, sobre métricas diarias de actividad/tenedores/emisión, a 24h.
   **NO cierra "on-chain"**: los flujos de exchange no se pudieron medir (§2), y esa es
   la parte con más mecanismo de las tres.

2. **Un sobreviviente que sea SOLO nivel crudo, y no sobreviva en su forma `_pct` o `_z`,
   se descarta.** Está rankeando qué cadena es, no qué está pasando. Es la lección de la
   corrida 4 escrita de antemano.

3. **Todo se corre a 0,20% y a 0,50%.** Un sobreviviente que solo vive a 0,20% no cuenta.

4. **Si el único sobreviviente es `CapMVRVCur`**, hay que compararlo contra los brazos de
   precio del mismo panel. Si `roc_*` o `dd_*` hacen lo mismo, MVRV no aporta: es precio
   con otro nombre.

5. **Corte por régimen, obligatorio para cualquier sobreviviente**: los cuatro tramos de
   la corrida 3 (bear 2021-11→2022-11, bull 2022-12→2024-03, lateral 2024-03→2025-08,
   bear 2025-08→2026-08), contra **200 rankings al azar por tramo** (la versión estricta
   de la corrida 4, no la de un solo control). **No puede cambiar de signo entre tramos**,
   y no puede caer a la mediana del azar en el tramo más reciente — que fue exactamente
   como murió `tt_pos`.

6. **Concentración**: `sin_top3` y `sin_top1`, ya cableadas. Con 41 activos y k=8, tres
   símbolos son el 37% de una selección: la compuerta importa más que nunca acá.

## 7. Lo que esta corrida NO puede decir

1. **Nada sobre la cola.** El universo son 43 activos que existían en 2021 y siguen
   cotizando: es un panel **viejo, grande y con sesgo de supervivencia**. Es la parte del
   mercado donde más se esperaría que no haya nada.
2. **Nada sobre flujos de exchange** (§2).
3. **Nada a horizonte corto.** Las métricas son diarias; a 24h ya se está usando un dato
   que se publica cada 24h. Horizontes más cortos no son medibles con esta fuente.
4. **No modela impacto de mercado.** Es un ranking, no un backtest.

## 8. ⚠️ Contabilidad: la reserva OOS ya no está virgen para este diseño

`PREREGISTRO_ANCHO.md` declaró la reserva **2024-08-01 → 2025-08-01** como no mirada,
con esta justificación textual: *"el pin `base200` y todos los lotes arrancan en
2025-08-01"*. Era cierto **cuando se escribió** (2026-08-24).

**Ya no lo es.** Las corridas 3 y 4 usaron paneles de cinco años (2021-08 → 2026-08) que
**la contienen**, y esta corrida 6 también. Así que para la familia del **ranking
transversal** esa ventana es in-sample y **no sirve como confirmación out-of-sample**.

Sigue virgen solo para los diseños que arrancan en 2025-08-01 (los lotes de `lote.py` y
`lote_ancho.py`). Esto **se escribe acá y va al handoff**, porque un OOS que se cree
virgen y no lo está es peor que no tener OOS.

## 9. Fuga declarada — lo que ya vi

- Vi **0 direccionales** en precio (4.140), en derivados (276) y en perpetuos (592). El
  sesgo apunta a **querer encontrar algo**, no a no encontrarlo.
- Vi el n, el MDE y la demora de publicación — a propósito, porque el handoff manda
  medirlos primero. **No vi ni un solo spread de ningún brazo on-chain.**
- Vi la lista de las 48 monedas y sus fechas de inicio. No vi ningún valor de ninguna
  métrica salvo los de la muestra de 5 activos × 27 días del self-test, que se usó para
  medir la demora de publicación.

---

# RESULTADOS DE LA CORRIDA 6 — on-chain (2026-08-28)

Panel de **41 activos · 1.795 barras · 257 semanas · 70.615 filas** con on-chain (93,3%
de cobertura), 2021-08-01 → 2026-08-01. `k=8`, paso 24h, horizonte 24h, sin solape.
**213 rankings × 3 objetivos × 2 costos = 1.278 brazos.** MDE ±0,065 ATR.

## R1. Dirección: 0 de 420, y ni un solo spread positivo

| costo | objetivo | brazos | spread > 0 | **SOBREVIVEN** | mejor |
|---|---|---|---|---|---|
| 0,20% | largo | 210 | **0** | **0** | −0,1160 |
| 0,20% | corto | 210 | **0** | **0** | −0,0790 |
| 0,50% | largo | 210 | **0** | **0** | −0,3404 |
| 0,50% | corto | 210 | **0** | **0** | −0,2948 |

**No es "murió en una compuerta": no hay ni un candidato.** Ni uno de los 420 brazos
direccionales tiene spread positivo — ni las direcciones activas, ni los tenedores, ni las
transacciones, ni la emisión, en ninguna de sus cinco formas (nivel, cambio a 7d y 30d, z
y percentil), en las dos direcciones, sobre 257 semanas y cuatro regímenes.

Las reglas 6.2 (nivel crudo vs `_pct`) y 6.4 (MVRV contra los brazos de precio) **no
llegaron a aplicarse**: no hubo nada que descalificar.

## R2. Magnitud: replica, y lo interesante es CUÁNTO queda al sacarle el kline

60 de 210 brazos sobreviven en magnitud, **33 de ellos on-chain puros**. Pero el mejor
on-chain es peor que el mejor del kline:

| fuente | mejor brazo | spread |
|---|---|---|
| precio | `roc_72` | **+1,136** |
| flujo del kline | `turnover` | +1,128 |
| flujo del kline | `n_surge` | +1,123 |
| **on-chain puro** | **`TxCnt_chg7`** | **+0,641** |
| MVRV (híbrida) | `CapMVRVCur_chg7` | +0,807 |

**MVRV muere por la regla 6.4**: `roc_72` (+1,136) le gana, o sea que no hace nada que los
brazos de precio del mismo panel no hagan. Es precio con otro nombre, como el preregistro
anticipaba.

### Lo que sí es un resultado: on-chain es MENOS redundante con el kline que el propio kline

Residualizando cada brazo contra `n_surge` **dentro de cada barra** (que es lo que el
radar ya usa en producción):

| brazo | crudo | sin `n_surge` | **cuánto queda** |
|---|---|---|---|
| `roc_72` | +1,136 | +0,732 | 64% |
| `turnover` | +1,128 | +0,473 | **42%** |
| **`TxCnt_chg7`** | +0,641 | **+0,453** | **71%** |
| `TxTfrCnt_chg30` | +0,589 | +0,434 | **74%** |
| `AdrActCnt_chg7` | +0,566 | +0,422 | **75%** |
| `TxCnt_pct` | +0,548 | +0,216 | 39% |

> **Las actividades de cadena en forma de CAMBIO retienen 71-75% de su poder al sacarles
> `n_surge`; `turnover` —una feature de la propia vela— retiene 42%.** O sea que
> `TxCnt_chg7` no es un proxy peor de lo mismo: es **otro eje**. Es información
> incremental sobre lo que el radar ya mide.

Nótese el contraste dentro de la propia familia on-chain: las formas de **cambio** (`_chg7`,
`_chg30`) retienen 71-75%, las de **percentil** (`_pct`) retienen 39%. El percentil contra
la propia historia se parece mucho más a lo que `n_surge` ya captura.

### El corte por régimen — lo pasa

Spread de magnitud por tramo (top-8 contra el universo de la misma barra):

| tramo | `TxCnt_chg7` | `TxCnt_chg7` sin `n_surge` | `n_surge` | CONTROL azar |
|---|---|---|---|---|
| bear 2021-11 → 2022-11 | +0,596 | +0,463 | +0,941 | −0,001 |
| bull 2022-12 → 2024-03 | +0,863 | +0,657 | +1,432 | −0,005 |
| lateral 2024-03 → 2025-08 | +0,396 | +0,269 | +0,774 | −0,025 |
| bear 2025-08 → 2026-08 | +0,727 | +0,454 | +1,303 | −0,030 |
| **TODO** | **+0,641** | **+0,453** | +1,123 | −0,002 |

**Mismo signo en los cuatro tramos, incluido el bull grande, y el control clavado en
cero.** Aguanta el mismo estándar que le aplicamos al hallazgo de OI de la corrida 3.

> **Precisión sobre el control**: es la **mediana de 5 rankings al azar por tramo**, no
> los 200 de la regla 6.5. Los 200 son la exigencia para un **sobreviviente direccional**,
> y no hubo ninguno. Aun así 5 ya es más estricto que la corrida 3, que hizo su corte de
> magnitud contra un solo control, y la brecha acá es de más de 20× (+0,27/+0,66 contra
> −0,03/0,00), no de un margen que dependa de cuántos controles se sorteen.

## R3. Por qué esto NO se puede poner en el radar hoy — y el número es de cobertura

| universo | con on-chain |
|---|---|
| pares USDT spot vivos en Binance (485) | **48 — 9,9%** |
| `base200` | **24 de 200 — 12,0%** |

**El radar rankea el universo líquido entero y esta fuente cubre el 10%.** Peor: el 10%
que cubre son las cadenas **viejas y grandes** (btc, eth, ltc, bch, dash, doge, xrp, xlm,
ada, algo, dot, trx, zec…), y las monedas que el radar efectivamente elige —las que tienen
un `n_surge` alto— tienden a ser justo las nuevas y chicas, que **no tienen on-chain**.

> **No es una falla de señal, es un desajuste de cobertura.** La señal incremental existe y
> aguanta cuatro regímenes; el problema es que solo existe para la décima parte del
> universo, y es la décima parte donde menos pasa.

## R4. Veredicto

> **Dirección: cerrada. 0 de 420 brazos, sin un solo spread positivo, con 257 semanas y un
> MDE (±0,065) igual al que cerró la familia de derivados. Esto es "no está", no "no se
> pudo medir".**
>
> **Magnitud: on-chain aporta información incremental real —retiene 71-75% al sacarle
> `n_surge`, más que las propias features del kline— y aguanta los cuatro regímenes. Pero
> cubre el 9,9% del universo operable, y justo la parte equivocada.**

**Lo que esta corrida NO cierra**, y hay que repetirlo porque es la parte con más
mecanismo: **los flujos de exchange**. `FlowInExNtv`/`FlowOutExNtv` existen para 2 activos
de 6.093 en el tier gratis. La idea titular de 4.3 sigue sin medirse, y para medirla hace
falta una fuente paga (Glassnode, Nansen, CryptoQuant) o reconstruirla desde Etherscan
etiquetando wallets de exchange a mano — que es una sesión entera y solo para ERC-20.

## R5. Lo que quedó abierto y es barato

**Probar `TxCnt_chg7` como brazo del radar sobre las 43 monedas que sí tienen cobertura.**
No como reemplazo de `n_surge` sino **encima**: el radar seguiría rankeando por `n_surge`
en todo el universo, y sobre el subconjunto con on-chain podría desempatar. Es incremental
(71%), aguanta 4 regímenes, y la fuente es gratis y sin API key.

Lo que **no** hay que hacer es meterlo como filtro global: dejaría afuera al 90% del
universo, que es exactamente donde el radar encuentra sus candidatos.
