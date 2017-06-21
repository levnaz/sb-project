#!/usr/bin/env python

# Imports the app variable from my app package and invokes its run method to start the server
# The app variable holds the Flask instance I created
from app import app
app.run(host='0.0.0.0', debug = True)
#app.run(host='0.0.0.0', port=80, debug = True)
