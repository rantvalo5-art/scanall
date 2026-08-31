# PREREGISTRO — corrida 14: la prima de volatilidad, medida DE COSTADO

> Escrito el **2026-08-30**, **antes de calcular un solo número transversal**.
> Dirección §6.3 de `HANDOFF_CUATRO.md`. Código: `opciones/potencia_transversal.py`.
> Resultados **debajo de la línea**.

---

## 1. Qué se pregunta, y por qué no es reabrir la corrida 8

La corrida 8 preguntó **"¿la prima media de volatilidad es positiva en alts?"** y murió por
potencia, no por el efecto:

- MDE **39,0%/año** en SOL contra un umbral preregistrado de 10%.
- Y la calibración es lo incontestable: **BTC con esa misma ventana da 27,1%/año**. Su
  efecto está medido y es conocido, y **tampoco habría sido detectable**. El estimador tiene
  una señal/ruido de ~1/5 por mes.
- Las tres salidas quedaron tapadas: delta-hedgear no baja la varianza (la σ usada ya es la
  del P&L hedgeado), esperar no alcanza, y **poolear subyacentes es la peor de las tres**:
  ρ = +0,92 entre el P&L mensual de las straddles de BTC, ETH, SOL y XRP, o sea que **4
  subyacentes son 1,07 independientes**.

**Lo que cambia acá es la forma funcional, no el dato.** Esa ρ = +0,92 es un **factor
común**: la volatilidad de cripto se mueve casi toda junta. En un test de series de tiempo
ese factor es el ruido que te mata. En un test **transversal** es lo que se **diferencia y
desaparece** — que es exactamente lo que hace el control por barra en todo el resto del
repo.

> **La pregunta pasa de "¿la prima media es positiva?" (pide 23 años) a "¿el subyacente con
> IV/RV más alto rinde MÁS que sus pares en la misma barra?".**

No es reabrir la corrida 8: es una pregunta distinta, sobre la sección cruzada, y el ruido
que cerró aquella está ausente por construcción en ésta.

**Prior: modesto.** Es una forma funcional que no se probó, y la aritmética dice que puede
quedar justo en el borde del umbral (§4). Puede no alcanzar. Por eso lo primero que corre es
la compuerta de potencia y no la medición.

---

## 2. El dato, y su límite, dicho antes

Índice de implícita a 30d de **Bybit**, en `opciones/iv_diaria/bybit_*.csv` (el colector
diario de PR #28). Cobertura al 2026-08-30:

| moneda | filas | desde |
|---|---|---|
| SOL | 567 | 2025-02-11 |
| BTC / ETH | 560 | 2025-02-18 |
| XRP | 321 | 2025-10-15 |
| DOGE | 314 | 2025-10-22 |
| HYPE | 33 | 2026-07-30 |

→ **3 nombres por ~18 meses, 5 por ~10 meses, 6 por 1 mes.** Es flaco y hay que decirlo de
entrada.

> **Una sola fuente de implícita para TODAS las monedas, y es Bybit.** BTC y ETH además
> tienen el DVOL de Deribit, que es más largo (2021) — y **no se usa acá**. Mezclar dos
> índices construidos con metodologías distintas dentro de la misma sección cruzada mete una
> diferencia **entre nombres** que es de método y no de mercado, y que el ranking leería como
> señal. El DVOL se usa solo para la calibración de §5, donde no cruza con nada.

---

## 3. La construcción, fijada acá

**Barra:** el mes, **no solapado**. Es la unidad independiente y es la que cuenta para el n.
Fecha de formación `t` = primer día de cada mes con dato para **≥ 3 monedas**.

**La señal, que tiene que ser observable en `t`:**

```
ratio_i(t) = IV_i(t) / RV_pasada_i(t)
```

donde `RV_pasada` es la vol realizada de los **30 días ANTERIORES** a `t`.

> ⚠️ **Esto NO es el `ratio` de `iv_rv.py`.** Aquel usa `rv` *futura* y es un diagnóstico
> ex-post, correcto para lo que hace allá y **lookahead** si se usara para rankear. Acá la
> señal solo puede mirar hacia atrás.

**El resultado, que es lo que se cobra** — misma fórmula que `iv_rv.py`, sin cambiarle nada:

```
vrp_i(t) = IV_i(t) - RV_futura_i(t)                      [puntos de vol]
pnl_i(t) = 0,7979 * vrp_i(t)/100 * sqrt(30/365) * 100    [% del spot]
```

`RV_futura` = vol realizada de los **30 días siguientes**, que es contra lo que se cobra de
verdad.

**La cartera, neutral por barra:**

```
cartera(t) = pnl del ratio MAS ALTO  -  pnl del ratio MAS BAJO
```

Con 3-6 nombres, top−bottom es más limpio e interpretable que pesos por rank. Se reporta
además la versión con **pesos por rank centrados** como secundaria, para que el cambio sea
auditable.

**La dirección se declara ACÁ, antes de medir: se espera POSITIVA.** El nombre con IV/RV más
alto cobra más prima por vender su volatilidad. (El repo midió dos veces que el brazo más
tentador estaba invertido — corrida 7, 3 de los 5 mejores; corrida 12, el mejor de todos.
Por eso el signo se escribe antes.)

**Costo:** 5% de la prima por pata, el mismo `COSTO_PRIMA` de `iv_rv.py`. **La cartera tiene
dos patas, así que paga el doble.** Se reporta a **5% y a 10%** por pata.

---

## 4. LA COMPUERTA, que corre ANTES de mirar el signo o el tamaño

Igual que `opciones/potencia.py` en la corrida 8: este paso **no mira la prima**. Calcula n y
σ.

```
MDE = 2,8 * sigma_mensual(cartera) / sqrt(n_meses) * 12        [%/año]
```

> ### (C) POTENCIA — si el MDE de la cartera transversal es **> 10%/año**, se declara **"no
> se pudo medir"** y la dirección se **cierra**. Mismo umbral que la corrida 8.

Y hay que **decir cuál de las dos falló**, porque una se arregla esperando y la otra no
(regla de la corrida 9: 266 semanas y aun así no pudo, porque decidía `σ/√n`):

- **por n** → se reabre cuando el colector diario junte más meses.
- **por σ** → no se reabre con más datos.

> ### (P) LA PREMISA — el diferencing tiene que estar haciendo algo
>
> Todo el argumento es que la ρ alta desaparece al diferenciar. Eso es **verificable, y se
> verifica antes de creerle al MDE**:
>
> **σ(cartera top−bottom) < σ(pnl de un nombre promedio).** Si la σ de la cartera no baja
> respecto de la de los nombres sueltos, el factor común no se removió, el argumento entero
> es falso y se cierra ahí — aunque el MDE diera lindo.
>
> Se reporta también la ρ media entre pares de nombres, para poder compararla con el +0,92
> de la corrida 8.

**La cuenta que dice por qué esto vale una tarde y no una semana, hecha antes de correr:**
BTC con 18 meses dio MDE 27,1%/año → σ mensual ≈ 3,42% del spot. Si el residual de la
cartera es `σ·√(2(1−ρ))` ≈ 0,40σ ≈ 1,37, entonces MDE ≈ **10,8%/año**. **Justo en el borde
del umbral de 10.** Puede pasar o puede no pasar, y esa es toda la razón para correrlo.

---

## 5. Calibración obligatoria

Se corre la misma cuenta sobre una serie con efecto **conocido**: BTC solo, con el DVOL
largo de Deribit. Si el aparato no reproduce lo que ya está medido en `iv_rv.py`, el
problema es el código y no el mercado. Sin esto, un número nuevo no se interpreta.

---

## 6. Solo si (C) y (P) pasan: los controles de la medición

Ninguno se afloja después de ver un número.

1. **Bootstrap de bloques por mes.** El p que decide es ése, no el que supone independencia.
   Cada mes pesa uno.
2. **Sin el mejor mes** y **sin el mejor par de nombres**. Con 3-6 nombres, sacar uno es el
   control de concentración que corresponde (el `sin_top3` del resto del repo no aplica acá).
3. **≥ 60% de meses positivos.**
4. **Los dos niveles de costo.**
5. **FDR sobre el lote entero** si se corre más de un brazo. Y no se corren variantes de la
   señal "a ver si con alguna sale": la señal es una y está fijada en §3.

---

## 7. Lo que este preregistro NO autoriza

- **Barrer el horizonte** (30d es el del índice; no se prueban 7d, 60d, 90d "a ver").
- **Barrer el universo** (entran todas las monedas con índice, no un subconjunto elegido).
- **Cambiar la señal a IV/RV futura** si la pasada no da: eso es lookahead.
- **Mezclar Deribit con Bybit** en la sección cruzada, por §2.
- **Aflojar el umbral de 10%/año** si el MDE da 11 o 12. Ese es exactamente el número que la
  aritmética de §4 anticipó, y aflojarlo después de verlo es fabricar el resultado.

---

# ────────────────── RESULTADOS (debajo de esta línea) ──────────────────

*(en blanco a propósito hasta que corra la compuerta)*
