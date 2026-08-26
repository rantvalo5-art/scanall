# PREREGISTRO — shock de OI durante CASCADA, en corto

> **Escrito ANTES de tocar los datos de prueba.** 2026-08-22.
> Si algo de este archivo cambia despues de ver un resultado, el experimento no vale.

## De donde salio, y por que necesita preregistro propio

La auditoria del 2026-08-22 (anexo de `PREREGISTRO_OI.md`) encontro que los criterios 3 y
7 del preregistro anterior se calcularon solo sobre semanas con >=20 senales, y que esas
semanas ganan 68,68% mientras las descartadas —el 55% de los trades— ganan 46,40%.

O sea: la regla ancha no queda sostenida, pero **adentro hay una mas angosta que si**. Esa
version angosta no la escribio nadie: la introdujo una constante del harness. Fue
**sugerida por los datos**, asi que el universo OOS-54 donde se la observo esta gastado y
no puede volver a usarse para validarla.

**Esto NO es un hallazgo todavia.** Este archivo existe para decidir si es real ANTES de
mirar.

## El mecanismo que se postula

El OI colapsa en una moneda sola por muchas razones (vencimiento, un ballena que cierra,
ruido de la fuente). El OI colapsa en **muchas monedas a la vez** por una sola: liquidacion
forzada de mercado entero. La hipotesis es que la senal vale cuando hay desapalancamiento
sincronizado, y no vale cuando una moneda suelta OI sola.

La variable observable es la **simultaneidad**, y se mide por HORA, no por semana. Esto
importa: la constante del harness contaba senales por semana, y una semana con 20 senales
repartidas en 20 horas distintas **no es una cascada**. La operacionalizacion de aca es
distinta —y a proposito mas fiel al mecanismo— que la que sugirio la idea.

## El umbral, derivado y no elegido

Bajo independencia, cada simbolo dispara `oi_z < -2` en el `p0 = phi(-2) = 2,275%` de las
horas. Si los simbolos fueran independientes, la cantidad que dispara en una hora dada
seria `Binomial(N, p0)`. Una cascada es un exceso sobre esa nula.

**Hora de cascada** = la cantidad de simbolos del universo con `oi_z < -2` en esa hora
cumple:

    k >= N*p0 + 2*sqrt(N*p0*(1-p0))          con p0 = 0,02275

El **2** no se elige: es el mismo que la senal ya usa en `oi_z < -2`. Se aplica la misma
convencion al eje transversal que la que ya estaba en el eje temporal. La formula escala
sola con el tamano del universo y se computa EN VIVO (sabes cuantos estan disparando
ahora).

Propiedades declaradas de antemano, para que no se discutan despues:
- Con N=40 el umbral es **k >= 3**. Bajo independencia el 6,2% de las horas lo cruzarian;
  todo lo que exceda ese 6,2% es correlacion real.
- Es un umbral **permisivo en terminos absolutos**. Es el precio de heredar el 2 en vez de
  ajustarlo. Si la regla necesitara una barra mucho mas dura para funcionar, falla — y eso
  tambien es informacion.

## La regla, exacta y ejecutable

- **Universo**: pares USDT spot con perpetuo en Binance Futures.
- **Regimen**: `close_1h(BTCUSDT) < EMA168(close_1h(BTCUSDT))`.
- **Senal**: `oi_z < -2` sobre ventana de 168h del cambio horario de
  `sum_open_interest_value`.
- **Cascada** (NUEVO): la hora de la senal cumple `k >= N*p0 + 2*sqrt(N*p0*(1-p0))`.
- **Accion**: SHORT al cierre de la vela en que se cumplen las TRES condiciones.
- **Salida**: triple barrera simetrica +8% / -8%, maximo 7 dias.
- **Costo**: 0,20% ida y vuelta.

## Datos de prueba — no usados para esta idea

El universo **`metricas40`** (los 40 simbolos del descubrimiento).

Justificacion: `metricas40` produjo la regla ANCHA, pero **nunca se lo miro por cascada** —
la particion por actividad se observo entera sobre el OOS-54. Lo que se testea aca es
especificamente si **la particion replica en un universo donde no se la busco**.

Ventana: 2021-08-01 -> 2026-08-01, la misma.

## Limitaciones declaradas de antemano

1. **`metricas40` no es una hoja en blanco.** Ya se sabe que ahi vive el efecto ancho. El
   test no puede decir "existe la senal"; solo puede decir "la particion por cascada
   separa, o no separa, donde no se la fue a buscar".
2. **Son las mismas semanas otra vez.** Igual que el preregistro anterior: out-of-sample en
   la seccion transversal, no en el tiempo.
3. **La operacionalizacion no es la que sugirio la idea.** Cascada por hora != 20 senales
   por semana. Si falla, puede ser porque la definicion horaria es la equivocada y no
   porque no haya efecto. **Se declara ahora para que no se use de excusa despues**: si
   falla, se cierra igual. No se re-operacionaliza.

## Criterios de aprobacion — los SIETE, todos obligatorios

Identicos a los del preregistro anterior, con **una correccion**: los criterios 3 y 7 se
calculan con el filtro `n >= 20` de `lote.py` **DESACTIVADO** (todas las semanas con al
menos una senal). Ese filtro es exactamente lo que la auditoria encontro roto.

1. `n >= 200`
2. win rate **>** el umbral necesario
3. **p por bloques semanales <= 0,10**, sobre TODAS las semanas
4. le gana a la linea base pareada del mismo simbolo (`vs_par > 0`)
5. margen sin los 3 simbolos que mas aportan `> 0`
6. margen sin el mejor simbolo solo `> 0`
7. **>= 60% de las semanas** arriba del umbral, sobre TODAS las semanas

**Si falla CUALQUIERA de los siete, se cierra.** Una sola corrida. No se re-corre con otro
universo, otra ventana, otro umbral de z, otra definicion de cascada ni otro horizonte.

### Diagnostico declarado (NO es compuerta)

Se reporta la distribucion de senales por semana. Si la regla esta bien formada, la
condicion de cascada deberia concentrar las senales y dejar pocas semanas flacas. Si
quedan muchas semanas de 1-2 senales, la definicion horaria no esta capturando el
mecanismo. Se mira, se anota, **no cambia el veredicto de esta corrida.**

## Prerrequisito de implementacion

`lote.py` tiene el 20 hardcodeado en dos lugares (`:157` dentro de `_p_bloques` y `:232` en
la compuerta semanal). Hay que parametrizarlo antes de correr. **El cambio va primero, y
se verifica reproduciendo un resultado viejo conocido** — no se toca el harness y se corre
el test en el mismo movimiento.

## Si aprueba

No alcanza para poner capital: sigue siendo la misma ventana temporal. Alcanza para
mandarlo al forward test en semanas nuevas, que es el unico pendiente real desde el
principio.

---

## ANEXO — corrida NULA del 2026-08-22 (defecto de implementacion)

La primera ejecucion de `test_cascada.py` **no vale**. No implemento la regla escrita: la
cascada se conto agrupando por `t` sobre la tabla de ENTRADAS, y las entradas estan
escalonadas por simbolo (31.953 timestamps distintos donde la grilla de 4h tendria
~10.950). Resultado: `N` mediano = **2** en vez de 21, el umbral colapso a `k >= 1`, y
cualquier simbolo disparando solo contaba como cascada. Filtro 2.838 -> 2.336 senales, o
sea casi nada.

**Por que se declara nula y no fallida.** El criterio es si el defecto se identifica sin
mirar el resultado. `N mediano = 2` con 23 simbolos en el universo es un error mecanico,
se imprime ANTES del veredicto y no depende de lo que hizo el win rate. Corregido: la
cascada se calcula sobre la grilla completa simbolo x hora (43.814 horas, N mediano 21,
tasa de disparo 2,42% contra 2,28% teorico).

**Costo declarado, y no es cero.** Los numeros de la corrida nula se vieron (57,94%,
p=0,8475, 47% de semanas). No se pueden des-ver. La corrida corregida ya no es tan limpia
como el preregistro pretendia: se sabe que pinta tiene el resultado de la version
casi-sin-filtrar. La mascara corregida es muy distinta, asi que el costo es chico — pero
queda anotado aca y no se descubre despues.

Sigue valiendo **una sola corrida** de la version corregida.

---

## RESULTADO — 2026-08-22. NO APRUEBA. CERRADO.

Corrida unica de la version corregida, sobre `metricas40`:

```
horas distintas 43.814 | N mediano 21 -> umbral mediano k>=2
horas de cascada 4.168 (9,5%) | bajo independencia se esperaria ~8,2%
senal sola 4.896 | + bajista 2.838 | + cascada 2.280
```

| criterio | valor | |
|---|---|---|
| 1. n >= 200 | n=1.795 | OK |
| 2. win rate > umbral | +6,74 pp | OK |
| **3. p bloques <= 0,10** | **p=0,9210** | **FALLA** |
| 4. le gana al pareado | +7,06 pp | OK |
| 5. sin top-3 > 0 | +4,94 pp | OK |
| 6. sin el mejor > 0 | +6,26 pp | OK |
| **7. >= 60% de semanas** | **46%** | **FALLA** |

Diagnostico declarado: 188 semanas con senal, mediana 6 senales/semana, 26% de las
semanas con 1-2 senales.

## Lectura

**Fallan los mismos dos criterios que la version ancha, y por lo mismo.** El agregado se
ve muy bien —57,99%, +7,06pp contra el dardo pareado, aguanta sacar los top-3 simbolos—
pero semana a semana el 46% esta arriba del umbral y el p por bloques da 0,9210. La semana
tipica pierde; el promedio lo sostienen unas pocas semanas gordas.

**La cascada resulto casi inerte**: filtro 2.838 -> 2.280 senales, un 20%. Con N=21 el
umbral de 2 sigma sobre la nula de independencia cae en k>=2, que es una barra baja. Estaba
declarado de antemano que ese es el precio de heredar el 2 en vez de ajustarlo, y que si la
regla necesitaba una barra mas dura, fallaba. Fallo.

**La contaminacion resulto irrelevante.** La corrida nula dio 57,94% / p=0,8475 / 47%; la
corregida dio 57,99% / p=0,9210 / 46%. Practicamente lo mismo, porque la mascara cambio
poco. El costo declarado en el anexo anterior no llego a importar.

## Consecuencia

El OI shock queda **CERRADO entero** — ancho y angosto. No se re-operacionaliza, no se
prueba otro umbral de cascada, no se cambia de universo. Estaba escrito antes.

Lo que la familia deja como aprendizaje: el efecto agregado es real y grande, pero vive en
un punado de periodos de alta actividad, y **ninguna regla que dispare sobre senales
individuales lo captura**. Operarlo significaria aguantar tramos largos de perdida para
agarrar esas semanas, y la evidencia semanal no puede distinguir eso de ruido.
