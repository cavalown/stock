from flask import Flask
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)


class StockWebsite(Resource):
    def get(self):
        return {'hello': 'Stocks'}


api.add_resource(StockWebsite, '/')
if __name__ == '__main__':
    app.run()
