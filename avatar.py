import pika, sys, os, json
from pika.spec import BasicProperties
import yaml
import requests

AMQP_SERVER = 'debianvm'
AMQP_EXCHANGE = 'mars'
TEMPO = 10

PROXY_HOST = '127.0.0.1'
PROXY_PORT = 8000
URL = f'http://{PROXY_HOST}:{PROXY_PORT}/numericRegister/single'

# list of the possible manipulation
manipulation_list = ["LOAD", "UNLOAD"]

# possible values for register 150 and 151
with open("MARS_avatar.yaml", 'r') as stream:
    eqt_list = yaml.safe_load(stream)

def build_query(register):
  return f'?reg={register}&type=int'

def build_body(eq_code):
  return {
    'data':{
      'value' : eq_code
    }
  }

def get_para_for_eq(manipulation, eq_type, eq_ref):
  if manipulation == 'LOAD':
    eq_code = eqt_list[eq_type]['references'][eq_ref]
    register = eqt_list[eq_type]['registerID']
  else :
    eq_code = eqt_list[eq_type]['references']['NONE']
    register = eqt_list[eq_type]['registerID']
  return eq_code, register

def callback(ch, method, properties, msg):
    body=json.loads(msg)
    headers = properties.headers
    
    path = headers['path']
    publisher = headers['publisher']

    print("message send on", path, "by", publisher)

    if path == '/sequencer/manipulation':
      
      uid = headers['uid']
      
      manipulation = body["operation"]
      eq_type = body["equipment"]["type"]
      eq_ref = body["equipment"]["reference"]
      
      if manipulation in manipulation_list:

          headers = {'publisher':'hmi', 'uid': uid}
          
          prop = BasicProperties()
          prop.headers = headers

          ch.basic_publish(exchange='mars',
                           routing_key="report.sequencer.hmi",
                           properties=prop,
                           body='{"status":"SUCCESS"}')

          # prepare description
          desc = "{} {} {}".format(body["operation"],
                                  body["equipment"]["type"],
                                  body["equipment"]["reference"])
          
          # print description
          print(" [x] {} in progress".format(desc))

          # get equipment code and register to update
          eq_code, register = get_para_for_eq(manipulation,
                                              eq_type,
                                              eq_ref)
          # time.sleep(TEMPO)

          #build url for proxy request
          url = URL+build_query(register)
          #build body for proxy request
          body = build_body(eq_code)
          print("request on url ", url)
          print(body)
          print(f"send request to proxy with id {uid}")
          response = requests.put(url=url,
                                  json=body,
                                  headers={'Content-Type':'application/json',
                                  'uid': uid})

          print(response.status_code)

          response.close()

          # ch.basic_publish(exchange='mars', routing_key="hmi.proxy.request", 
          #                 body=json.dumps(cde_request(val, reg, desc)))
          
          # print(" [x] Sent to proxy service : %s" % json.dumps()
      else:
        print(" [x] unknown action")
        headers = {'publisher':'hmi', 'uid': uid}
          
        prop = BasicProperties()
        prop.headers = headers

        ch.basic_publish(exchange='mars',
                        routing_key="report.sequencer.hmi",
                        properties=prop,
                        body='{"status":"ERROR", error: "unknown action"}')

def main():   
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=AMQP_SERVER))
    channel = connection.channel()
    channel.exchange_declare(exchange=AMQP_EXCHANGE, exchange_type='topic')

    result = channel.queue_declare('',
                                   exclusive=False,
                                   auto_delete=True)

    queue_name = result.method.queue
    channel.queue_bind(exchange='mars', queue=queue_name, routing_key="request.hmi")
    channel.queue_bind(exchange='mars', queue=queue_name, routing_key="report.hmi")


    print(' [*] Waiting for request from sequencer. To exit press CTRL+C')    
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
    connection.close()
    print(" [*] connection closed")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

