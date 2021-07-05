import pandas as pd
from flask import Flask
from flask_restful import Resource, Api, reqparse

######################
# Import Dataset     #
######################

dataset = pd.read_csv("nba_dataset.csv")

######################
# Initialize Flask   #
######################

app = Flask(__name__)
api = Api(app)


######################
# Configure API      #
######################

class NBA(Resource):
    def get(self):
        """
        Get the NBA statistic data.
        Optional parameter: sort_age -> Boolean
        :return: [dictionary, code]
        """
        global dataset

        parser = reqparse.RequestParser()
        parser.add_argument("sort_age", type=bool, store_missing=False)
        args = parser.parse_args(strict=True)

        rtn = dataset

        if "sort_age" in args and args['sort_age']:
            rtn = dataset.sort_values(by='age', ascending=True, ignore_index=True)

        return {
                   'data': rtn.to_dict()
               }, 200

    # noinspection DuplicatedCode
    def post(self):
        """
        Insert new row of data into the database
        :return: [dictionary, code]
        """
        global dataset

        parser = reqparse.RequestParser()
        parser.add_argument("last", type=str, required=True)
        parser.add_argument("first", type=str, required=True)
        parser.add_argument("age", type=float, required=True)
        parser.add_argument("height", type=float, required=True)
        parser.add_argument("wingspan", type=float, required=True)
        parser.add_argument("weight", type=int, required=True)
        parser.add_argument("gp", type=int, required=True)
        parser.add_argument("mp", type=int, required=True)

        args = parser.parse_args(strict=True)

        target = dataset[dataset['last'] == args['last']]

        if not target.empty and args['first'] in list(target['first']):
            return {
                       'message': "{} {} found in the database".format(args['last'], args['first'])
                   }, 409

        newRow = pd.DataFrame({
            'last': [args['last']],
            'first': [args['first']],
            'age': [args['age']],
            'height': [args['height']],
            'wingspan': [args['wingspan']],
            'weight': [args['weight']],
            'gp': [args['gp']],
            'mp': [args['mp']]
        })

        dataset = dataset.append(newRow, ignore_index=True)

        return {
                   'data': dataset.to_dict()
               }, 200

    # noinspection DuplicatedCode
    def put(self):
        """
        Insert a new row if not exists else override the entire row.
        :return: [dictionary, code]
        """

        global dataset

        parser = reqparse.RequestParser()
        parser.add_argument("last", type=str, required=True)
        parser.add_argument("first", type=str, required=True)
        parser.add_argument("age", type=float, required=True)
        parser.add_argument("height", type=float, required=True)
        parser.add_argument("wingspan", type=float, required=True)
        parser.add_argument("weight", type=int, required=True)
        parser.add_argument("gp", type=int, required=True)
        parser.add_argument("mp", type=int, required=True)

        args = parser.parse_args(strict=True)

        idx = dataset.query(f"last == \"{args['last']}\" and first == \"{args['first']}\"").index.tolist()

        newRow = pd.DataFrame({
            'last': [args['last']],
            'first': [args['first']],
            'age': [args['age']],
            'height': [args['height']],
            'wingspan': [args['wingspan']],
            'weight': [args['weight']],
            'gp': [args['gp']],
            'mp': [args['mp']]
        })

        if idx:
            dataset.iloc[idx[0]] = newRow.iloc[0]
        else:
            dataset = dataset.append(newRow, ignore_index=True)

        return {
                   'data': dataset.to_dict()
               }, 200

    def delete(self):
        """
        Delete the record of given lastname and firstname
        :return: [dictionary, code]
        """

        global dataset

        parser = reqparse.RequestParser()
        parser.add_argument('last', type=str, required=True)
        parser.add_argument('first', type=str, required=True)

        args = parser.parse_args(strict=True)

        idx = dataset.query(f"last == \"{args['last']}\" and first == \"{args['first']}\"").index.tolist()

        if idx:
            dataset = dataset.drop(idx)

            return {
                       'data': dataset.to_dict()
                   }, 200

        return {
                   'message': "{} {} is not exists.".format(args['last'], args['first'])
               }, 404

    def patch(self):
        """
        Update the existing record with given lastname and firstname
        :return: [dictionary, code]
        """
        global dataset

        parser = reqparse.RequestParser()
        parser.add_argument('last', type=str, required=True)
        parser.add_argument('first', type=str, required=True)
        parser.add_argument("age", type=float, store_missing=False)
        parser.add_argument("height", type=float, store_missing=False)
        parser.add_argument("wingspan", type=float, store_missing=False)
        parser.add_argument("weight", type=int, store_missing=False)
        parser.add_argument("gp", type=int, store_missing=False)
        parser.add_argument("mp", type=int, store_missing=False)

        args = parser.parse_args(strict=True)

        idx = dataset.query(f"last == \"{args['last']}\" and first == \"{args['first']}\"").index.tolist()

        if idx:
            args.pop('last')
            args.pop('first')

            for key, value in args.items():
                dataset.at[idx[0], key] = value

            return {
                       'data': dataset.to_dict()
                   }, 200

        return {
                   'message': "{} {} is not exists.".format(args['last'], args['first'])
               }, 404


######################
# Add Endpoint       #
######################

api.add_resource(NBA, '/nba')

if __name__ == '__main__':
    app.run()
