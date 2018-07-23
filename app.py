from cassandra.cluster import Cluster
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify
from flask_cors import CORS, cross_origin

cluster = Cluster()


app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'

#http://127.0.0.1:5000/insert_id?epc=10198a853f71
@app.route("/insert_id", methods=['GET', 'POST'])
def insert_id():
	session = cluster.connect('ci')
	session.set_keyspace("ci")
	id = request.args.get('epc');

	select = "SELECT * FROM epc WHERE epc=%s"
	p = session.execute(select,[bytearray(id,'utf8')])

	print(not p)

	status = "old"
	if not p:
		status = "new"


	insert = "INSERT INTO epc (epc, status) VALUES (%s, %s)"
	r = session.execute(insert, [bytearray(request.args.get('epc'), 'utf8'), status])

	return status


#http://127.0.0.1:5000/unread
@app.route("/unread", methods=['GET', 'POST'])
@cross_origin(origin='localhost',headers=['Content- Type'])
def unread():
	
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		return "no entries in epc yet!"

	for obj in p:
		insert = "INSERT INTO epc (epc, status) VALUES (%s, %s)"
		r = session.execute(insert, [bytearray(obj.epc,'utf8'), 'unread'])

	j = dict()
	j['res']='marked all as read'

	return jsonify(j)


#http://127.0.0.1:5000/missing
@app.route("/missing", methods=['GET', 'POST'])
def missing():
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		return "no entries in epc yet!"

	a = []
	for obj in p:
		if(obj.status == 'unread'):
			a.append(obj.epc);		

	return " ".join(str(x) for x in a)


#http://127.0.0.1:5000/new
@app.route("/new", methods=['GET', 'POST'])
def new():
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		return "no entries in epc yet!"

	a = []
	for obj in p:
		if(obj.status == 'new'):
			a.append(obj.epc);		

	return " ".join(str(x) for x in a)


#http://127.0.0.1:5000/old
@app.route("/old", methods=['GET', 'POST'])
def old():
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		return "no entries in epc yet!"

	a = []
	for obj in p:
		if(obj.status == 'old'):
			a.append(obj.epc);		

	return " ".join(str(x) for x in a)

if __name__ == '__main__':
    app.run()
