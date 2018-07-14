from cassandra.cluster import Cluster
from flask import Flask
from flask import request

cluster = Cluster()


app = Flask(__name__)

#http://127.0.0.1:5000/insert_id?epc=10004a853fe0&lastreadtime=2018-07-14%2003:39:37.881
@app.route("/insert_id", methods=['GET', 'POST'])
def cassandra_test():
	session = cluster.connect('ci')
	session.set_keyspace("ci")
	session.execute("INSERT INTO epc (epc, lastreadtime) VALUES (%s, %s)", [request.args.get('epc'), request.args.get('lastreadtime')])
	r = session.execute("SELECT * FROM TEST");
	return str(r[0])  

if __name__ == '__main__':
    app.run()
