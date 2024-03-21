#!/usr/bin/env python

# import de liberias necesarias
import os
import time
import logging
import json
from datetime import datetime
from kafka import KafkaProducer

# obtenemos la ruta de trabajo
path_base = os.getcwd()

# leemos fichero de configuracion
file_script = os.path.basename(__file__)
file_config = os.path.join(path_base, file_script.replace('.py','.cfg'))

f = open(file_config, 'r')
data = json.loads(f.read())

server = data['server']
topic = data['topic_name']
time_inter_files = data['time_inter_files']
time_standby = data['time_standby']
to_send = data['folders'][0]
to_process = data['folders'][1]
received = data['folders'][2]
failed = data['folders'][3]

# se crea un log para mostrar los errores
log = logging.getLogger(__name__)

# se crea el Kafka Producer en el unico broker que hay
# en el cual se serializan los mensajes como JSON
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=lambda m: json.dumps(m).encode('utf-8'))


# se deja esta funcion para mostrar los registros procesados correcamente
def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


# se deja esta funcion para mostrar mensajes de error
def on_send_error(ex):
    log.error('I am an Error', exc_info=ex)
    # handle exception


# se crean un contador que sera la key del mensaje
num_line = 0


# vemos si existe la carpeta de entrada de ficheros
# si no existe la crea
path_to_send = os.path.join(path_base, to_send)
if os.path.isdir(path_to_send):
    pass
else:
    os.mkdir(path_to_send)

# vemos si existen las carpetas de salida
# si no existen las crea
path_processed = os.path.join(path_base, to_process)
if os.path.isdir(path_processed):
    pass
else:
    os.mkdir(path_processed)

path_received = os.path.join(path_base, received)
if os.path.isdir(path_received):
    pass
else:
    os.mkdir(path_received)

path_failed = os.path.join(path_base, failed)
if os.path.isdir(path_failed):
    pass
else:
    os.mkdir(path_failed)

# bucle infinito para chequear carpeta de entrada de ficheros
while True:

    # vemos si hay ficheros que procesar
    list_txt = []
    for lf in os.listdir(path_to_send):
        if lf.endswith('.txt'):
            list_txt.append(lf)

    # si hay ficheros que procesar, los envia
    if list_txt:
        for lt in list_txt:
            # inicializamos variables
            num_err = 0  # sera el contador de registros que han dado error
            list_ok = []  # lista para almacenar registros procesdos correctamente
            list_ko = []  # lista para almacenar registros que han dado error

            # leemos un fichero de la carpeta de entrada
            with open(os.path.join(path_to_send, lt), 'r', encoding='utf-8') as f:
                for line in f:
                    try:

                        key = str(num_line)

                        tweet_lab = line.split(',')[2][0:3].upper()

                        if '"' in line:
                            tweet_msg = (line.split('"')[1]).strip()
                        else:
                            tweet_msg = (''.join(line.split(',')[3:])).strip()

                        # si la lectura del registro va bien, lo envia
                        producer.send(topic='tweets-kafka', key=key.encode('utf-8'),
                                      value={'msg': str(tweet_msg)}).add_callback(on_send_success).add_errback(
                            on_send_error)

                        num_line += 1

                        # y lo guarda en la lista de registros correctos
                        temp = '{"label": "' + tweet_lab + '", "tweet": "' + tweet_msg + '"}'

                        list_ok.append(json.loads(temp))
                    except:
                        # si no se es capaz de leer el registro, se vuelca a la lista de error
                        print('Fallo la lectura de {}'.format(str(line)))
                        list_ko.append(line)
                        continue

            # guardamos el timestamp para que los ficheros con registros ok y ko sean iguales
            now = datetime.now()

            # guardamos en fichero los mensajes analizados correctamente
            if list_ok:
                file_ok = os.path.join(path_received, now.strftime('%Y%m%d-%H%M%S') + '_reg_ok.csv')
                file = open(file_ok, 'w')
                for lok in list_ok:
                    try:
                        file.write(str(lok) + '\n')
                    except:
                        continue
                file.close()

            # si hay registros erroneos se guarda fichero para analizar que fallo
            if list_ko:
                file_ko = os.path.join(path_failed, now.strftime('%Y%m%d-%H%M%S') + '_reg_ko.csv')
                file = open(file_ko, 'w')
                for lko in list_ko:
                    file.write(lko + '\n')
                file.close()

            # movemos el fichero analizado a processados
            try:
                os.rename(os.path.join(path_to_send, lt), os.path.join(path_processed, lt))
            except:
                os.rename(os.path.join(path_to_send, lt), os.path.join(path_processed, now.strftime('%Y%m%d-%H%M%S') + '_' + lt))


            # se hace el volcado a memoria
            producer.flush()

            # se cierra el producer
            # producer.close()

            print('File {} analized.'.format(str(os.path.join(path_to_send, lt))))
            print('-------')

            # tras enviar un fichero, espera time_inter_files segundos antes de pasar al siguiente
            time.sleep(int(time_inter_files))

    # si no hay fichero, espera time_standby segundos para chequear directorio
    else:
        time.sleep(int(time_standby))
