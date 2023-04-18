from datetime import date
from sqlite3 import Timestamp
from flask import Flask,jsonify,request
import psycopg2

database = psycopg2.connect(
    database="bigdatatest",
    host="localhost",
    user="geraldakbar",
    password='bigdata'
)

app = Flask(__name__)
app.secret_key = "DEVELOPMENT_SECRET_KEY"
app.config['SESSION_PERMANENT'] = False


@app.route("/")
def index():
    return jsonify({'Test':'Data'})

@app.route("/test")
def test():
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        socmed = request.args.get('socmed')

        cur = database.cursor()
        print(socmed)
        query = f"SELECT * FROM social_media_data WHERE timestamp > {start} AND social_media={socmed}"

        ret = []
        cur.execute(query)
        records = cur.fetchall()
        for record in records:
            ret.append({
                'Social Media': record[0],
                'Date': record[1],
                'Count': record[5],
                'Unique Count': record[2],

            })

        return jsonify({'start':start,'end':end,'social_media':socmed,'record': ret})
    except psycopg2.Error as e:
        print("Error")
        database.rollback()
    finally:
        cur.close()

if __name__ == '__main__':
    app.run(debug=True,port=3000,host='0.0.0.0')