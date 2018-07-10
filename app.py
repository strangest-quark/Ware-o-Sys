from cassandra.cluster import Cluster
from flask import Flask
from flask import request

cluster = Cluster()


app = Flask(__name__)

@app.route("/insert_id", methods=['GET', 'POST'])
def cassandra_test():
	session = cluster.connect('ci')
	session.set_keyspace("ci")
	session.execute("INSERT INTO test (id) VALUES (%s)", [int(request.args.get('id'))])
	r = session.execute("SELECT * FROM TEST");
	return str(r[0])  

if __name__ == '__main__':
    app.run()
