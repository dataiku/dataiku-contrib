# -*- coding: utf-8 -*-
from dataiku.customrecipe import *
import requests, json, time
import dataiku_esri_content_utils

def recipe_config_get_str_or_none(param_name):
    v = get_recipe_config().get(param_name, None)
    if v is not None and len(v) == 0:
        v =None
    return v

def get_token_from_login_password(P_USERNAME,P_PASSWORD,P_TOKEN_EXPIRATION):
    print("Authorizing ...")
    URL = 'https://www.arcgis.com/sharing/rest/generateToken'
    resp = requests.post(URL, data= {
        'username':P_USERNAME,
        'password':P_PASSWORD,
        'client':'referer',
        'referer':'http://www.arcgis.com',
        'f':'json',
        'expiration': P_TOKEN_EXPIRATION
    })

    if resp.status_code != 200:
        raise Exception("Failed to authorize to ESRI http_code=%s resp=%s" % (resp.status_code, resp.text))

    resp_json = resp.json()
    if "error" in resp_json:
        raise Exception("Failed to authorize to ESRI error=%s" % resp_json["error"])

    token =  resp_json["token"]
    expires = resp_json["expires"]
    tokenExpiresReadable = time.strftime('%Y-%m-%d %I:%M:%S %p (%Z)', time.localtime(expires/1000))

    print("Authorized, Token expires on %s" % tokenExpiresReadable)
    return token,tokenExpiresReadable

def get_coverage_dict(P_COUNTRY_MODE):
  # Build the dictionary of dicts
  dict_esri_coverage = dataiku_esri_content_utils.get_esri_coverage()
  dict_esri_coverage_structure = dict()

  for i in range(0, len(dict_esri_coverage['country'])):
      co = dict_esri_coverage['country'][i]
      i3 = dict_esri_coverage[u'isocode3'][i]
      i2 = dict_esri_coverage[u'isocode2'][i]

      structure = {u'attributes':
                       {u'isocode3': i3
                        ,u'isocode2': i2
                        ,u'country': co
                        ,u'comment': dict_esri_coverage[u'comment'][i]
                        ,u'content_as_of': dict_esri_coverage[u'esri_content_as_of'][i]
                        ,u'datacollections': {
                            u'generic_datacollections': dict_esri_coverage[u'generic_datacollections'][i]
                        }}}

      if P_COUNTRY_MODE =='full_name':
          dict_esri_coverage_structure[co]=structure
      elif P_COUNTRY_MODE =='isocode3':
          dict_esri_coverage_structure[i3]=structure
      elif P_COUNTRY_MODE =='isocode2':
          dict_esri_coverage_structure[i2]=structure

  return dict_esri_coverage_structure