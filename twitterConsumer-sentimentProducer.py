#!/usr/bin/env python

# import de liberias necesarias
import os
#import time
import json
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
from datetime import datetime

# obtenemos la ruta de trabajo
path_base = os.getcwd()

# leemos fichero de configuracion
file_script = os.path.basename(__file__)
file_config = os.path.join(path_base, file_script.replace('.py','.cfg'))

f = open(file_config, 'r')
data = json.loads(f.read())

model_0 = data['model_0']
model_1 = data['model_1']
model_2 = data['model_2']
in_use = data['in_use']
size_limit = data['size_limit']
topic_consumer = data['topic_consumer']
group_name = data['group_name']
server = data['server']
offset_type = data['offset_type']
topic_producer = data['topic_producer']
to_backup = data['folders'][0]
regs_in_file = data['regs_in_file']


# se crea el pipeline que ejecuta el modelo de analisis de sentimientos
if in_use == '0':
    tweets_pipeline = pipeline(model_0)
elif in_use == '1':
    tweets_pipeline = pipeline(model=model_1)
else:
    print('INFO: Se usa el modelo basico.')
    tweets_pipeline = pipeline('sentiment-analysis')


# se abre la conexión del consumer de kafka indicando:
# - el topic al que se va subscribir (se podría dejar en blanco)
# - el grupo de consumidores que van a recibir los mensajes del producer
# - donde esta el servidor de mensajes
# - 'earliest' es el comando de python que se traduce al 'frombeginnig' de kafka
consumer = KafkaConsumer(topic_consumer,
                         group_id=group_name,
                         bootstrap_servers=[server],
                         auto_offset_reset=offset_type)

# se indica el topic al que se subscribe el consumer
consumer.subscribe([topic_consumer])

# para poder hacer el analisis de sentimientos creamos un nuevo producer 
# que enviará por un nuevo topic via stream los datos a ksqldb
producer = KafkaProducer(bootstrap_servers=[server])
topic_name = topic_producer

# se va a crear un fichero con los mensajes recibidos por el consumer
# y se guardan en una carpeta
path_base = os.getcwd()
path_analysis_in = os.path.join(path_base, to_backup)
if os.path.isdir(path_analysis_in):
    pass
else:
    os.mkdir(path_analysis_in)

# para ello, se crea un fichero por cada 50 mensajes
counter = 0
list_msg = []

# se muestra por pantalla diversa informacion:
# - informacion del topic
# - particion y el offset del broker
# - la key del mensaje
# - el mensaje que se ha analizado
# - y el sentimiento de cada mensaje
for message in consumer:
    counter += 1
    key_counter = str(counter).encode('utf-8')
    if len(message.value.decode('utf-8')) < int(size_limit):
        print("%s:%d:%d: key=%s tweet=%s value=%s" % (message.topic,
                                                      message.partition,
                                                      message.offset,
                                                      message.key.decode('utf-8'),
                                                      message.value.decode('utf-8'),
                                                      tweets_pipeline([message.value.decode('utf-8')])))

        sentiment_dict = {
            'label': tweets_pipeline([message.value.decode('utf-8')])[0]['label'],
            'score': round(float(tweets_pipeline([message.value.decode('utf-8')])[0]['score']),4),
            'tweet': message.value.decode('utf-8')[8:-1].replace('"', '')
        }
    else:
        sentiment_dict = {
            'label': 'OUT',
            'score': 0,
            'tweet': message.value.decode('utf-8')[8:-1].replace('"', '')
        }
    print(sentiment_dict)

    sentiment_toSend = json.dumps(sentiment_dict).encode('utf-8')

    producer.send(topic=topic_name, key=key_counter, value=sentiment_toSend)
    producer.flush()

    list_msg.append(sentiment_dict)


    if counter % int(regs_in_file):
        pass
    else:
        now = datetime.now()
        file_consumer = os.path.join(path_analysis_in, now.strftime('%Y%m%d-%H%M%S') + '_reg_consumer.csv')
        file = open(file_consumer, 'w')
        for lm in list_msg:
            file.write(str(lm) + '\n')
        file.close()

        list_msg = []
        tic = datetime.now()