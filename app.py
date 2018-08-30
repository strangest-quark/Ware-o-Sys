from cassandra.cluster import Cluster
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify
from flask_cors import CORS, cross_origin

cluster = Cluster()


app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'

#http://ec2-54-212-43-165.us-west-2.compute.amazonaws.com/insert_id?epc=10198a853f71
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

	j = dict()
	j['res']=status

	return jsonify(j)


#http://ec2-54-212-43-165.us-west-2.compute.amazonaws.com/delete_id?epc=10198a853f71
@app.route("/delete_id", methods=['GET', 'POST'])
def delete_id():
	session = cluster.connect('ci')
	session.set_keyspace("ci")
	id = request.args.get('epc');

	delete = "DELETE FROM epc WHERE epc=%s"
	r = session.execute(delete, [bytearray(request.args.get('epc'), 'utf8')])

	j = dict()
	j['res']=r

	return jsonify(j)


#http://ec2-54-212-43-165.us-west-2.compute.amazonaws.com/notread
@app.route("/notread", methods=['GET', 'POST'])
@cross_origin(origin='localhost',headers=['Content- Type'])
def notread():
	
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		j = dict()
		j['res']="no entries to mark notread found"
		return jsonify(j)

	for obj in p:
		insert = "INSERT INTO epc (epc, status) VALUES (%s, %s)"
		r = session.execute(insert, [bytearray(obj.epc,'utf8'), 'notread'])

	j = dict()
	j['res']='marked all as read'

	return jsonify(j)


#http://ec2-54-212-43-165.us-west-2.compute.amazonaws.com/missing
@app.route("/missing", methods=['GET', 'POST'])
def missing():
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		j = dict()
		j['res']="no missing entries found"
		return jsonify(j)

	a = []
	for obj in p:
		if(obj.status == 'notread'):
			a.append(obj.epc);		

	j = dict()
	j['res']=" ".join(str(x) for x in a)

	return jsonify(j)


#http://ec2-54-212-43-165.us-west-2.compute.amazonaws.com/new
@app.route("/new", methods=['GET', 'POST'])
def new():
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		j = dict()
		j['res']="no new entries found"
		return jsonify(j)

	a = []
	for obj in p:
		if(obj.status == 'new'):
			a.append(obj.epc);		

	j = dict()
	j['res']=" ".join(str(x) for x in a)

	return jsonify(j)


#http://ec2-54-212-43-165.us-west-2.compute.amazonaws.com/old
@app.route("/old", methods=['GET', 'POST'])
def old():
	session = cluster.connect('ci')
	session.set_keyspace("ci")

	select = "SELECT * FROM epc"
	p = session.execute(select)

	if not p:
		j = dict()
		j['res']="no old entries found"
		return jsonify(j)

	a = []
	for obj in p:
		if(obj.status == 'old'):
			a.append(obj.epc);		

	j = dict()
	j['res']=" ".join(str(x) for x in a)

	return jsonify(j)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)
