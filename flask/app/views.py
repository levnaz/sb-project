import base64
import authdata # imports my credentials
import json

from Crypto.Cipher import AES
from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify
from flaskext.mysql import MySQL

from app import app


def stringtokey(string):
    # Converts string key to byte key
    bytekey = string.encode('utf_8')
    key = base64.b64decode(bytekey)
    return key

def unpad(paddedText):
    # Removes the padded '{'s
    start = paddedText.find('{')
    unpadded = paddedText[0:start]
    return unpadded

def aesdec(cipherText, key):
    # Decryps a string with a given key
    cipher = AES.new(key)
    cipherBytes = cipherText.encode('utf_8')
    decoded = base64.b64decode(cipherBytes)
    decrypted = cipher.decrypt(decoded)
    padded = decrypted.decode('utf_8')
    plainText = unpad(padded)
    return plainText

def db_connection():
    # Creates a DB connection and returns the cursor

    # MySQL configurations
    mysql = MySQL()
    app = Flask(__name__)
    app.config['MYSQL_DATABASE_USER'] = authdata.MYSQL_DATABASE_USER
    app.config['MYSQL_DATABASE_PASSWORD'] = authdata.MYSQL_DATABASE_PASSWORD
    app.config['MYSQL_DATABASE_DB'] = authdata.MYSQL_DATABASE_DB
    app.config['MYSQL_DATABASE_HOST'] = authdata.MYSQL_DATABASE_HOST
    mysql.init_app(app)

    # http://127.0.0.1:5000/Authenticate?UserName=jay&Password=jay
    #username = request.args.get('UserName')
    #password = request.args.get('Password')
    cursor = mysql.connect().cursor()
    return cursor

# Create the mappings from urls / and /index to this function
# In other words, tell Flask what URL should trigger our function
@app.route('/')
@app.route('/index')
def index():
  return render_template('index.html')

@app.route('/hello_world')
def hello_world():
    return 'Hello, World (hello_world)!'


@app.route('/user/<username>')
def show_user_profile(username):
    # show the user profile for that user
    return 'User %s' % username

@app.route('/post/<int:post_id>')
def show_post(post_id):
    # show the post with the given id, the id is an integer
    return 'Post %d' % post_id


@app.route('/projects/')
def projects():
    return 'The project page'

@app.route('/about')
def about():
    return 'The about page'


@app.route('/db_enc')
def db_enc():
    cursor = db_connection()
    sql = 'SELECT * FROM t_range_m LIMIT 2'

    cursor.execute(sql)
    data = cursor.fetchall()

    # Data has the following columns in DB
    # 'recordID': recordID,
    # 'c1': mac_address_enc,
    # 'c2': access_point_enc,
    # 'c3': internalNodesStr,
    # 'c4': stay_enc

    recordID = str()
    mac_address_enc = str()
    access_point_enc = str()
    internalNodesStr = str()
    stay_enc = str()    
    db_item = dict()

    for row in data:
        recordID = row[0]
        mac_address_enc = row[1]
        access_point_enc = row[2]
        internalNodesStr = row[3]
        stay_enc = row[4]

        if recordID not in db_item:
            db_item[recordID] = [mac_address_enc, access_point_enc, internalNodesStr, stay_enc]

    return render_template('db_enc.html', data = data)

@app.route('/db_plain')
def db_plain():
    cursor = db_connection()
    sql = 'SELECT * FROM t_range_m LIMIT 20'

    cursor.execute(sql)
    data = cursor.fetchall()

    # Data has the following columns in DB
    # 'recordID': recordID,
    # 'c1': mac_address_enc,
    # 'c2': access_point_enc,
    # 'c3': internalNodesStr,
    # 'c4': stay_enc

    recordID = str()
    mac_address = str()
    access_point = str()
    internalNodesStr = str()
    stay = str()
    db_item = dict()

    for row in data:
        recordID = row[0]
        mac_address = aesdec(row[1], stringtokey(authdata.AESKEY))
        access_point = aesdec(row[2], stringtokey(authdata.AESKEY))
        internalNodesStr = row[3]
        stay = aesdec(row[4], stringtokey(authdata.AESKEY))
        if recordID not in db_item:
            db_item[recordID] = [mac_address, access_point, internalNodesStr, stay]
    return render_template('db_plain.html', db_item = db_item)

@app.route('/getFruits')
def my_view():
    data = ['Banana', 'Apple', 'Orange', 'Pear', 'Watermalon'] # you can get list from your DB instead
    return render_template('fruits.html', data=json.dumps(data))

