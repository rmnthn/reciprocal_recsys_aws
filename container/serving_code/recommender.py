# Implements a flask server to do inference for a single user

from __future__ import print_function

import os
import json
import pickle
import sys
import signal
import traceback
import new_user_recommender

import flask

import pandas as pd


import random, itertools, gzip, math, json
import numpy as np
import annoy

prefix = '/opt/ml/'
model_path = os.path.join(prefix, 'model')

# A singleton for holding the model. This simply loads the model and holds it.
# It has a recommend function that does a prediction based on the model and the input data.

class RecommendationService(object):
    model = None                # Where we keep the model when it's loaded

    @classmethod
    def get_model(cls):
        """Get the model object for this instance, loading it if it's not already loaded."""
        if cls.model == None:
            cls.model = {}
            
            # Load preferences_vectors
            
            # Load profiles

            # Load annoy indexes
           
        return cls.model

    @classmethod
    def recommend(cls, rec_input):
        """
        For the input, generate the recommendations and return them.

        Args:
            input (json_file): The data on which to do the predictions
        """
        rec_model = cls.get_model()
        
        preference_vectors = new_user_recommender.generate_new_preferences(rec_input['user_id'], rec_input['gender'], rec_input['locationId'], rec_input['birthdate'], rec_input['followingCategories'], rec_model)
        recommendations = new_user_recommender.generate_recommendations(rec_input['user_id'], preference_vectors, rec_model, 100)            

        return recommendations


# The flask app for serving predictions
app = flask.Flask(__name__)

@app.route('/ping', methods=['GET'])
def ping():
    """
    Determine if the container is working and healthy. In this sample container, we declare
    it healthy if we can load the model successfully.
    """
    health = RecommendationService.get_model() is not None  # You can insert a health check here

    status = 200 if health else 404
    return flask.Response(response='\n', status=status, mimetype='application/json')

@app.route('/invocations', methods=['POST'])
def transformation():
    """
    Do inference on a single new user. In this server, we take user data as json, convert
    it to python dict for internal use and then convert the recommendations back to JSON.
    """
    data = None

    # Convert from JSON to dict
    if flask.request.content_type == 'application/json':
        data = flask.request.data.decode('utf-8')
        data = json.loads(data)
        # print(data)
    else:
        return flask.Response(response='This predictor only supports JSON data', status=415, mimetype='text/plain')

    # Generate recommendations
    recommendations = RecommendationService.recommend(data)

    # Convert to JSON
    recommendations_json = json.dumps(recommendations)
    # print(recommendations_json)
    
    return flask.Response(response=recommendations_json, status=200, mimetype='application/json')
