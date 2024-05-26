from flask import Flask
import flask.scaffold
flask.helpers._endpoint_from_view_func = flask.scaffold._endpoint_from_view_func

from flask_restful import Api, Resource
class Multiplication(Resource): 
	def get(self, num1, num2):
		return {'result': float(num1) * float(num2)}

app = Flask(__name__)
api = Api(app)
api.add_resource(Multiplication, '/<num1>/<num2>')

if __name__ =="__main__":
	app.run(
		debug=True,
		port=5053,
		host="0.0.0.0"
	)
