from flask import Flask, request, render_template, url_for, jsonify
from flask_cors import CORS

import _live_crawler as crawler
import os
import json

app = Flask(__name__)
app.debug = True

#cors = CORS(app, resources={r"/api/*": {"origins": "*"}})
CORS(app)

@app.route('/')
def home():

    #return render_template("search_job.html")
    return "Hello There"

@app.route('/search_data', methods=['GET', 'POST'])
def scrape_data():

    data = {"got": "Nothing"}
    if request.method == 'POST':

        data = request.form.to_dict()
        
        key = data['job']
        loc = data['location']
        
        print(f"\n\tKey : {key}, loc: {loc}\n\n")
        
        f_name = crawler.f_start+"_"+key.replace(" ", "_").replace(",", "_")+"_"+loc.replace(" ", "_").replace(",", "_")
        if os.path.exists(f_name+"_linked_in_data.txt"):
            return jsonify({'result': 1, 'status': 'exists', 'path': f_name})
        else:
            crawler.main(key=key, loc=loc)
            return jsonify({'result': 1, 'status': 'created', 'path': f_name})

    return jsonify({'result': 0, 'status': 'failed'})

@app.route('/get_file', methods=['GET', 'POST'])
def get_file():

    if request.method == 'POST':

        data = request.form.to_dict()
        data['start'] = int(data['start'])
        return_data = []
        exit_stat = False
        if os.path.exists(data['f_name']+"_linked_in_summary.txt"):
            exit_stat = True
            i = -1
            for line in open(data['f_name']+"_linked_in_summary.txt", 'r'):
                i+=1
                if i < data['start']:
                    continue
                if i == data['start']+5:
                    break
                return_data.append(json.loads(line))

        if os.path.exists(data['f_name']+"_indeed_summary.txt"):

            exit_stat = True
    
            i = -1
            for line in open(data['f_name']+"_indeed_summary.txt", 'r'):
                i+=1
                if i < data['start']:
                    continue
                if i == data['start']+5:
                    break
                return_data.append(json.loads(line))

        if len(return_data) == 0 and not exit_stat:
            return {'result':0, 'msg':'NO jobs found'}

        return {'result':1, 'data':return_data}

    return {'result':-1}
