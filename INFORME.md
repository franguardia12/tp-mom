# Informe de implementación

La solución se implementó en Python utilizando el cliente Pika y respetando las
interfaces provistas en `common/middleware/middleware.py`. Este documento se
limita a describir las decisiones de diseño, los supuestos adoptados y el manejo
de recursos y errores de la implementación.

## Estructura de la solución

La lógica compartida se concentró en la clase privada
`_MessageMiddlewareRabbitMQ`. Esta clase administra la conexión, el canal, el
consumo, los ACK/NACK, la publicación, la detención y el cierre. Las clases
concretas solamente definen la topología particular de una cola de trabajo o de
un exchange. Esta organización evita duplicar lógica y permite mantener toda la
solución dentro del único archivo Python admitido por la entrega.

Cada instancia crea una `BlockingConnection` y un canal propios. No se crean
threads ni procesos internos y no se comparte una conexión entre instancias.
Esto también evita compartir conexiones de Pika entre distintos contextos de
ejecución.

## Middleware de cola

`MessageMiddlewareQueueRabbitMQ` declara una cola clásica con el nombre recibido
en el constructor. La cola es durable y los mensajes se publican como
persistentes mediante el exchange predeterminado, usando el nombre de la cola
como routing key.

La cola no es exclusiva ni se elimina al cerrar una instancia porque puede ser
compartida por varios productores y consumidores. Eliminarla desde uno de ellos
podría descartar mensajes pendientes o afectar al resto de las instancias.

Se configura `prefetch_count=1` para que un consumidor no acumule entregas sin
confirmar mientras otros consumidores estén disponibles.

## Middleware de exchange

`MessageMiddlewareExchangeRabbitMQ` utiliza un exchange básico de tipo `direct`
y durable. Se eligió `direct` porque las routing keys de la interfaz representan 
coincidencias exactas. El broadcast se obtiene vinculando varias colas con una 
misma key, sin necesitar un exchange `fanout` ni un protocolo de control adicional.

Las routing keys se validan, se copian a una tupla y se eliminan duplicados
preservando el orden. Se asumió que una instancia configurada con varias keys
debe publicar el mensaje una vez por cada una de ellas. Para consumir, la misma
instancia vincula su cola con todas las keys configuradas.

La cola consumidora se crea de manera diferida en `start_consuming()`. De este
modo, una instancia utilizada únicamente para publicar no crea una cola ni
bindings innecesarios. RabbitMQ genera el nombre de la cola, que es no durable y
exclusiva de la conexión. Se usa `auto_delete=False` para que una llamada a
`stop_consuming()` no impida volver a consumir con la misma instancia; el
carácter exclusivo hace que la cola sea eliminada cuando se cierra la conexión.

No se elimina el exchange durante `close()`, ya que es un recurso compartido por
otras instancias.

## Entrega y confirmación de mensajes

El consumo utiliza confirmación manual (`auto_ack=False`). El callback interno
adapta la firma de Pika a la firma solicitada por la interfaz y entrega:

- El cuerpo recibido, sin transformarlo.
- Una función `ack()` asociada al `delivery_tag` de esa entrega.
- Una función `nack()` asociada al mismo `delivery_tag`.

Ambas confirmaciones se ejecutan sobre el canal que recibió el mensaje. `nack()`
solicita la reencolación para permitir que otro consumidor vuelva a procesarlo.
La decisión de confirmar o rechazar queda así en manos de la aplicación, una vez
que conoce el resultado del procesamiento.

No se habilitaron publisher confirms. La interfaz exige informar errores de la
operación de publicación, pero no define una garantía adicional de confirmación
entre productor y broker.

## Manejo de errores

Los errores de Pika se traducen a las excepciones definidas por la interfaz:

- Los errores o estados cerrados de conexión se informan como
  `MessageMiddlewareDisconnectedError`.
- Los errores de declaración, publicación, consumo, bindings y ACK/NACK se
  informan como `MessageMiddlewareMessageError`.
- Los errores ocurridos al liberar recursos se informan como
  `MessageMiddlewareCloseError`.
- En `stop_consuming()`, una desconexión se informa como
  `MessageMiddlewareDisconnectedError`; los demás fallos de detención, incluido
  un canal cerrado, como `MessageMiddlewareCloseError`.

Las excepciones traducidas conservan el error original como causa mediante
encadenamiento de excepciones. Si falla la inicialización después de abrir una
conexión, se intenta cerrarla; un eventual error de limpieza queda agregado al
error original en lugar de ocultarlo.

Las excepciones producidas por el callback de la aplicación no se capturan como
errores internos del middleware, ya que hacerlo ocultaría un fallo ajeno a la
abstracción de comunicación. Se identifican en el adaptador del callback para
preservar su identidad incluso si su tipo coincide con una excepción de Pika.
Los errores de ACK/NACK ya traducidos también conservan su tipo y causa.

Las operaciones internas capturan las desconexiones antes de una captura general
de `Exception`, que traduce los restantes errores ordinarios sin silenciarlos.
Si falla el consumo o la configuración de la cola privada, se intenta cerrar la
conexión para liberar los recursos y las entregas pendientes. Después de esa
salida por error debe crearse una nueva instancia. La detención normal conserva
la conexión y permite volver a consumir. Las interrupciones como
`KeyboardInterrupt` y `SystemExit` provocan limpieza al salir del bucle de consumo
y se propagan sin convertirse en errores del middleware. Si varias operaciones
de cierre fallan, se conserva la primera causa y se agregan las siguientes como
notas.

`MessageMiddlewareDeleteError` está declarado en el archivo de interfaces, pero
ningún método abstracto solicita eliminar una cola o un exchange ni documenta
que pueda elevar ese error. Por ese motivo no se incorporó una eliminación
explícita. Las colas compartidas y los exchanges no deben ser eliminados por una
instancia individual, mientras que la cola exclusiva es eliminada por RabbitMQ
al cerrar su conexión.

## Ciclo de vida y concurrencia

`stop_consuming()` no tiene efecto si la instancia no está consumiendo, tal como
indica la interfaz. `close()` también es idempotente y, aun si una operación de
cierre falla, intenta liberar los demás recursos antes de elevar el error.

La implementación no introduce concurrencia interna. Cada objeto y su conexión
se utilizan desde un único thread. Por lo tanto, no existen secciones críticas 
compartidas que requieran locks dentro del middleware. El acceso concurrente a 
una misma instancia desde varios threads queda fuera de este modelo, en concordancia 
con las restricciones de `BlockingConnection` de Pika.

## Alcance y supuestos

- El constructor sólo recibe `host`; para puerto, credenciales y virtual host se
  utilizan los valores predeterminados de Pika porque la interfaz no permite
  configurarlos.
- Se utilizan únicamente colas clásicas, exchanges directos y operaciones AMQP
  básicas.
- No se implementan reconexiones ni reintentos automáticos. Ante una desconexión
  se informa el error correspondiente para evitar ocultar fallos o introducir
  duplicaciones mediante reintentos no solicitados.
- Si un mensaje se publica en un exchange antes de que exista un binding
  coincidente, RabbitMQ puede descartarlo. Este comportamiento es consistente
  con una suscripción basada en colas exclusivas y no se agrega almacenamiento
  alternativo por fuera del broker.
