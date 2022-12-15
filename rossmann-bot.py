import os
import pandas as pd
import json
import requests
from flask import Flask, request, Response
# constants
TOKEN = '5864485480:AAEh9lA6b0901IoQ4Eab5Zhb6_Q--mRI2LU'
#info about the bot
#https://api.telegram.org/bot5864485480:AAEh9lA6b0901IoQ4Eab5Zhb6_Q--mRI2LU/getMe
#get update metodo que consegue pegar a mensagem
#https://api.telegram.org/bot5864485480:AAEh9lA6b0901IoQ4Eab5Zhb6_Q--mRI2LU/getUpdates
#webhook
#https://api.telegram.org/bot5864485480:AAEh9lA6b0901IoQ4Eab5Zhb6_Q--mRI2LU/setWebhook?url=
#send message
#https://api.telegram.org/bot5864485480:AAEh9lA6b0901IoQ4Eab5Zhb6_Q--mRI2LU/sendMessage?chat_id=5648744740&text=hi

#5648744740


def send_message(chat_id,text):
	url = 'https://api.telegram.org/bot{}/'.format( TOKEN )
	url = url + 'sendMessage?chat_id={}'.format(chat_id)
	#metodo de requisição de envio de dados passando o texto que nos queremos como input resultante da função preditora
	r = requests.post(url, json={'text':  text})
	print('Status Code{}'.format(r.status_code))
	#não retorna nada pois so estou enviando a função
	return None




def load_dataset(store_id):

	# loading test dataset
	df10 = pd.read_csv( 'test.csv' )
	df_store_raw = pd.read_csv('store.csv', low_memory = False)
	# merge test dataset + store
	df_test = pd.merge( df10, df_store_raw, how='left', on='Store' )

	# choose store for prediction
	df_test = df_test[ df_test['Store']== store_id ]
	if not df_test.empty:
	
		# remove closed days
		df_test = df_test[ df_test['Open'] != 0 ]
		df_test = df_test[ ~df_test['Open'].isnull() ]
		df_test = df_test.drop( 'Id', axis=1 )

		# convert Dataframe to json
		data = json.dumps( df_test.to_dict( orient='records' ) )
	else:
		data = 'error'
		
	return data


def predict(data):

	 #API Call
	url = 'https://rossmann-api-hyjx.onrender.com/rossmann/predict'
	header = {'Content-type': 'application/json' } 
	data = data

	r = requests.post( url, data=data, headers=header )
	print( 'Status Code {}'.format( r.status_code ) )

	d1 = pd.DataFrame( r.json(), columns=r.json()[0].keys() )
	return d1

def parse_message(message):
	chat_id = message['message']['chat']['id']
	store_id = message['message']['text']
	#todo comando do telegram vem com uma barra logo tem que substituir
	store_id = store_id.replace('/','')
	#o comando tem que ser numero caso contrario temos que sinalizar o erro
	try:
		store_id = int(store_id)
	except ValueError:
		
		store_id = 'error'
	return chat_id, store_id

#API initialize	
app = Flask( __name__ )
#cria endpoint na raiz mesmo e permitir o metodo get e post juntamente com a FUNÇÃO INDEX QUE VAI RODAR SEMPRE QUE O ENDPOINT '/' OU APP.ROUTE FOR ACIONADO
@app.route('/', methods = ['GET','POST'])
def index():
	#se for pegar a messagem
	if request.method == 'POST':
		#pegar messagem do json 
		message = request.get_json()
		#pegar chat_id e store_id da mensagem
		#criar função pra pegar a messagem
		chat_id, store_id = parse_message(message)
		
		if store_id != 'error':
			#loading
			data = load_dataset(store_id)
			
			if data != 'error':
				
				#prediction
				#calcular predição através do modelo salvo em nuvem que pode ser rodado a qualquer momento
				d1 = predict(data)
				#calculation por loja
				d2 = d1[['store', 'prediction']].groupby( 'store' ).sum().reset_index()
				#send message
				msg = 'Store Number {} will sell R${:,.2f} in the next 6 weeks'.format( d2['store'].values[0], d2['prediction'].values[0] ) 
				send_message(chat_id,msg)
				return Response('Ok',status = 200)
				
			else:
				send_message(chat_id, 'Store Not Available')
				#tem que passar o status se não a API fica rodando sem fim
				return Response('Ok',status=200)
		else:
			send_message(chat_id, 'Store ID is Wrong')
			return Response ('Ok', status = 200)
			
			
	else:
		return '<h1> Rossmann Telegram BOT </h1>'
#5000 é a porta padrão do flask
if __name__ == '__main__':
	port = os.environ.get('PORT',5000)
	app.run(host = '0.0.0.0', port = port)
	
	
	

