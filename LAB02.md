# LAB 02 — Customer Data Ingestion con PySpark y Delta Lake

**Proyecto:** Retail Customer 360 · **Plataforma:** Databricks Free Edition · **Compute:** Serverless · **Duración estimada:** 90–120 minutos.

## 1. Qué vas a construir

Ingerirás una exportación sintética de CRM desde un repositorio público de GitHub, conservarás el archivo en un volumen de Unity Catalog y registrarás sus registros en `retail_customer360.bronze.customers_raw`, una tabla administrada Delta. Medirás problemas de calidad sin corregirlos ni eliminar registros originales.

Al finalizar podrás explicar un esquema explícito de PySpark, distinguir errores de formato de errores de negocio, comprobar una ingesta y justificar decisiones de trazabilidad e idempotencia.

```text
GitHub: CSV versionado + manifest
           │ GET de URL raw (Python)
           ▼
Unity Catalog Volume: copia exacta + checksum
           │ spark.read.csv
           ▼
DataFrame PySpark: 15 columnas string
           │ metadatos + escritura Delta
           ▼
retail_customer360.bronze.customers_raw
           │ perfilado y validación
           ▼
Silver: próximo laboratorio
```

La URL raw es el origen real de la descarga. Spark lee la copia del volumen: no se asume que `spark.read.csv("https://...")` funcione. Databricks recomienda volúmenes para archivos de ingesta y documenta su lectura con Spark. Free Edition usa serverless y restringe destinos de Internet; el acceso a `raw.githubusercontent.com` debe comprobarse en tu workspace. [Volúmenes](https://docs.databricks.com/aws/en/volumes/volume-files), [limitaciones Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

## 2. Estado de publicación y preparación de GitHub

**El intento de publicar mediante el conector falló con HTTP 403, `Resource not accessible by integration`. No hay commit publicado por esta tarea.** El repositorio estaba vacío cuando se consultó. Los enlaces raw siguientes están previstos y solo funcionarán después de subir los archivos.

Repositorio fuente para este y los futuros laboratorios: https://github.com/normanmartinez2/retail_customer360

Sube a la rama `main` el contenido de esta carpeta respetando esta estructura (no añadas el prefijo `lab02/`):

```text
data/crm/customers_lab02_v1.csv
data/crm/customers_lab02_v1.manifest.json
tools/generate_customers.py
labs/lab02_customer_ingestion.py
LAB02.md
README.md
```

Puedes usar GitHub → Add file → Upload files, o una copia local de tu repositorio. Si aún está vacío, crea primero un README para inicializar `main` y después sube las carpetas. Una alternativa es habilitar permiso de escritura de Contents para la integración GitHub y volver a solicitar la publicación.

URL raw prevista del CSV:

https://raw.githubusercontent.com/normanmartinez2/retail_customer360/main/data/crm/customers_lab02_v1.csv

URL raw prevista del contrato:

https://raw.githubusercontent.com/normanmartinez2/retail_customer360/main/data/crm/customers_lab02_v1.manifest.json

Después de publicar, abre ambas URL: el CSV debe mostrar una cabecera de 15 columnas y el JSON debe indicar `rows: 1220`. Copia el SHA del commit desde GitHub y cambia `REF = 'main'` por ese SHA en el notebook. Así todos ejecutarán exactamente la misma versión. El checksum detecta modificaciones accidentales; no demuestra autenticidad frente a alguien que pueda modificar tanto CSV como manifest.

## 3. Entender los datos

Son datos completamente ficticios. Los correos usan `example.com` y no representan clientes reales. Hay 1.200 registros base, diez repeticiones exactas y diez versiones posteriores de clientes existentes: total **1.220 filas lógicas**, sin contar cabecera. No cuentes líneas físicas: tres notas contienen saltos de línea.

| Columnas | Significado y decisión Bronze |
|---|---|
| customer_id | Identificador CRM; string, conserva prefijos y valores vacíos |
| first_name, last_name | Nombres con Unicode, espacios y apóstrofes |
| email, phone | Contacto ficticio; se conserva sin normalizar |
| date_of_birth | Texto ISO cuando es válido; incluye fechas imposibles y futuras |
| country, city, postal_code | Ubicación; códigos postales string para conservar ceros |
| customer_segment | Standard, Silver o Gold; incluye valor heredado fuera del contrato |
| marketing_consent | true/false previstos; YES y unknown requieren interpretación posterior |
| created_at, updated_at | Texto de timestamps; conversiones tolerantes solo para diagnóstico |
| source_system | CRM_SIMULATED |
| notes | Texto libre con comas, comillas y notas multilínea |

| Anomalía medible | Resultado esperado |
|---|---:|
| ID vacío | 10 |
| Email vacío | 15 |
| Email no vacío que falla la regla de forma | 12 |
| Grupos de ID no vacío duplicado | 20 |
| Filas adicionales por ID duplicado | 20 |
| Repeticiones exactas adicionales | 10 |
| Fecha de nacimiento imposible | 6 |
| Fecha de nacimiento futura frente a 2026-10-10 | 4 |
| Actualización anterior a creación | 6 |
| updated_at no convertible | 4 |
| Notas multilínea | 3 |

Además hay diez emails con espacios y mayúsculas, ocho nombres con espacios, siete países `es`, cinco `XX`, seis segmentos heredados, diez consentimientos no canónicos, cinco teléfonos vacíos y tres ciudades vacías. Las métricas no se suman para obtener “clientes malos”: un cliente puede fallar varias reglas. La expresión regular de email valida una forma básica, no la existencia ni la entregabilidad del correo.

## 4. Crear el notebook

1. En Databricks, abre Workspace y crea un notebook Python llamado `LAB02_Customer_Ingestion`.
2. Selecciona Serverless. No necesitas crear un cluster clásico.
3. Importa `labs/lab02_customer_ingestion.py` como notebook de formato fuente Databricks, o copia sus bloques a celdas Python. La última celda usa SQL.
4. Ejecuta en orden. No ejecutes dos copias simultáneamente.

Necesitas permisos para crear el catálogo, esquema y volumen y para leer/escribirlos y crear tablas. Si LAB 01 ya creó el catálogo, `IF NOT EXISTS` lo reutiliza. Si aparece un error de permisos, revisa el objeto concreto en Catalog Explorer; no sustituyas silenciosamente el catálogo objetivo.

## 5. Configuración y descarga

La primera celda centraliza el repositorio, referencia Git, URLs y nombres de objetos. La segunda crea catálogo, esquema y volumen. Un catálogo organiza esquemas; un esquema contiene tablas y volúmenes. El volumen guarda archivos, mientras que la tabla ofrece datos consultables.

La tercera celda descarga CSV y manifest usando `requests`, comprueba el estado HTTP, SHA-256, cabecera, número de registros y anchura de cada registro. Solo escribe al volumen después de validar. Conserva exactamente los bytes del CSV.

**Checkpoint:** debe imprimir 1.220 filas y este SHA-256:

`3a9847413e1ee3338d2df483be99331afd627db1096ea41e2438e81de8c672ab`

Un 404 significa ruta, referencia o publicación inexistente. Un error de red puede indicar restricciones de Free Edition. Si el dominio está bloqueado, descarga ambos archivos desde GitHub en tu ordenador, súbelos al volumen dentro de la carpeta `crm` y establece `MANUAL_UPLOAD = True`. La descarga automática sigue siendo la ruta principal; la alternativa usa el mismo origen versionado y verifica los mismos bytes.

## 6. Crear el DataFrame

La cuarta celda usa un `StructType` explícito con todas las columnas de origen como `StringType`. Un DataFrame es una colección distribuida de filas con esquema; sus transformaciones construyen un plan y acciones como `count()` ejecutan trabajo.

No usamos inferencia: un código `00007` no debe convertirse en número, y una fecha inválida debe seguir disponible para diagnóstico. Las celdas de perfilado crean columnas auxiliares tipadas sin sustituir las columnas originales.

`header=True` interpreta cabecera; `enforceSchema=False` permite comprobar su correspondencia con el esquema. `multiLine=True` conserva notas con saltos de línea y `escape='"'` interpreta comillas duplicadas producidas por el escritor CSV. `FAILFAST` detiene errores estructurales; no rechaza un email inválido porque su tipo sigue siendo string. Estas opciones siguen el lector CSV de Spark. [Referencia CSV](https://spark.apache.org/docs/latest/sql-data-sources-csv.html).

**Checkpoint:** `printSchema()` debe mostrar 15 strings y `count()` debe devolver 1.220. Busca `postal_code = '00007'` y comprueba que sigue teniendo cinco caracteres. Bronze conserva valores de campos sin limpieza; el archivo del volumen conserva además los bytes originales. El lector puede representar campos vacíos como null, por lo que validamos null y cadena vacía conjuntamente.

## 7. Perfilar sin limpiar

La quinta celda usa `try_cast` para que entradas inválidas produzcan null en columnas auxiliares, sin detener el análisis. Compara con las métricas del manifest usando assertions.

Distingue tres conceptos: ID duplicado, fila exacta repetida y nueva versión del mismo cliente. El primer caso identifica claves con más de una aparición; el segundo compara todas las columnas; el tercero requiere orden temporal y reglas de negocio.

**Ejercicio:** visualiza los registros `C000300` a `C000309`. Explica por qué no debes aplicar `dropDuplicates(['customer_id'])` sin un criterio de selección. Luego inspecciona los diez IDs vacíos: no representan un único cliente aunque todos tengan la misma clave ausente.

## 8. Escribir Bronze como Delta

La sexta celda añade fecha de ingesta, UUID del lote, URL, referencia Git y checksum. `saveAsTable` registra una tabla administrada: Unity Catalog gobierna su nombre y acceso; Delta proporciona el formato y registro transaccional. El archivo CSV y la tabla Delta son objetos distintos.

La escritura usa append para conservar el lote recibido, incluidos duplicados intencionales. Antes de insertar, comprueba si el mismo checksum ya existe. Al repetir el notebook secuencialmente con los mismos bytes, omite la escritura.

Esta política identifica contenido: dos exportaciones con bytes idénticos se consideran el mismo lote, aunque el CRM las envíe en fechas distintas. El patrón de consulta previa y append no es seguro ante concurrencia. En producción diseñaríamos identificadores de entrega, control transaccional de lotes y recuperación de fallos. Para este laboratorio ejecuta una sola instancia.

**No hagas MERGE todavía.** En Silver analizaremos claves, versiones y desempates antes de adaptar `DeltaTable.forName(...).merge(...)`. Si varias filas del origen actualizan una misma fila destino, un MERGE puede resultar ambiguo; la deduplicación debe tener reglas explícitas.

## 9. Validar el resultado

La séptima celda compara el lote almacenado con el DataFrame de origen mediante `exceptAll` en ambos sentidos. Esto verifica valores y multiplicidad, incluidos duplicados. Comprueba 1.220 filas para el checksum; si luego hay otros lotes, el total global puede ser mayor.

Consulta `DESCRIBE DETAIL` para confirmar `format = delta` y `DESCRIBE HISTORY` para observar la escritura. Abre Catalog Explorer → retail_customer360 → bronze → customers_raw y comprueba esquema, muestra y metadatos.

Reejecuta el notebook completo: debe indicar que el contenido ya fue ingerido, mantener 1.220 filas para este hash y no generar otra escritura. Los metadatos persistidos siguen siendo los de la primera ingesta. No borres la tabla para repetir el ejercicio.

## 10. Problemas frecuentes

| Síntoma | Qué revisar |
|---|---|
| 403 al publicar en GitHub | Permiso de Contents de la integración; no se arregla cambiando el notebook |
| 404 en requests | Publicación pendiente, carpeta equivocada o REF inexistente |
| Error de acceso al volumen | USE CATALOG, USE SCHEMA, READ/WRITE VOLUME y privilegios de creación |
| Más de 1.220 líneas al abrir CSV | Notas multilínea: contar registros con un lector CSV |
| Número de filas inesperado | multiLine, escape, checksum y versión |
| Error de checksum | No reformatees el CSV con Excel ni cambies finales de línea |
| Assertion tras escritura | Inspecciona lotes repetidos o ejecución concurrente antes de modificar datos |

## 11. Preparación para entrevista

**¿Por qué strings en Bronze?** Para mantener valores de origen y diagnosticar formatos antes de convertirlos. En Silver aplicaría tipos y contratos de negocio, con registros rechazados trazables.

**¿Qué aporta Delta sobre CSV?** Transacciones y un registro de versiones para la tabla. CSV queda como formato de intercambio del CRM; Delta como formato de almacenamiento consultable.

**¿Qué aporta Unity Catalog?** Un espacio de nombres y gobierno para los objetos de datos, con permisos sobre tablas y volúmenes. No es el motor que convierte CSV en Delta.

**¿Cómo evitas duplicar una ingesta?** En el laboratorio, checksum y comprobación previa en ejecución secuencial. Reconozco su límite ante concurrencia y distinguiría identidad de archivo de identidad de cliente.

**¿Por qué no eliminas los duplicados en Bronze?** Pueden ser reenvíos o versiones válidas. Los conservo y mido; resuelvo su significado en Silver.

**¿Cómo escalaría?** Sustituiría la descarga de un archivo pequeño con Python por almacenamiento de objetos y un mecanismo incremental apropiado. GitHub es útil como repositorio didáctico reproducible; no como plataforma de exportación de un CRM de producción.

**Respuesta de 60 segundos:** “Construí una ingesta batch desde un CSV versionado en GitHub. Verifiqué checksum y contrato, conservé el archivo en un volumen gobernado y lo leí con un esquema explícito en PySpark. Publiqué los registros originales en Bronze Delta con trazabilidad del lote. Validé valores y multiplicidad y detecté duplicados, fechas inválidas y campos ausentes. La reejecución secuencial evita insertar el mismo contenido; para producción reforzaría el control de concurrencia y trasladaría la fuente a almacenamiento de objetos.”

## 12. Convención para los siguientes laboratorios

GitHub será el repositorio fuente: `data/<sistema>/<dataset>_labNN_vN.csv`, manifest adyacente, generador en `tools/` y notebook en `labs/`. Publica nuevos archivos versionados en vez de sobrescribir el dataset de un ejercicio anterior. Cada manifest debe documentar esquema, filas, checksum, carácter sintético y métricas esperadas. Los notebooks usarán URLs raw fijadas a commit cuando se publique una versión estable.

Próximo paso: Silver, normalización, reglas de calidad, cuarentena y selección determinista de versiones. Después incorporaremos MERGE para nuevos clientes y actualizaciones.

## Estado de verificación

El dataset y su contrato se han validado localmente. El notebook contiene comprobaciones ejecutables para Databricks. No se ha ejecutado contra tu workspace y la publicación en GitHub quedó bloqueada por el 403 de la integración; por tanto no se presenta una URL raw como verificada remotamente.
