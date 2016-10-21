from simple_salesforce import Salesforce
from airflow.hooks.base_hook import BaseHook

import logging
import json

import pandas as pd

import os

class SalesforceHook(BaseHook):
    #https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/field_types.htm
    #http://www.chiragmehta.info/chirag/2011/05/16/field-datatype-mapping-between-oraclesql-server-and-salesforce/
    SF_TO_BQ_TYPE_MAP = {
        "boolean":      "BOOLEAN",
        "date":         "DATE",
        "datetime":     "DATETIME",
        "currency":     "FLOAT",
        "double":       "FLOAT",
        "int":          "INTEGER",
        "picklist":     "STRING",
        "id":           "STRING",
        "reference":    "STRING",
        "textarea":     "STRING",
        "email":        "STRING",
        "phone":        "STRING",
        "url":          "STRING",
        "multipicklist":"STRING",
        "anyType":      "STRING",
        "percent":      "STRING",
        "combobox":     "STRING",
        "base64":       "STRING",
        "time":         "TIME",
        "string":       "STRING"
    }

    def __init__(
            self,
            username,
            password,
            security_token = None,
            *args,
            **kwargs
        ):
        """
        Create new connection to Salesforce
        """
        self.username = username
        self.password = password
        self.security_token = security_token



    def signIn(self):
        """
        Sign into Salesforce.  If we have already signed it, this will just return the original object
        """
        if hasattr(self, 'sf'):
            return self.sf
        sf = Salesforce(username=self.username, password=self.password, security_token=self.security_token)
        self.sf = sf
        return sf

    def makeQuery(self, query):
        """
        Make a query to Salesforce.  Returns result in dictionary
        """
        if not hasattr(self, 'sf'):
            self.signIn()

        logging.info("Querying for all objects")
        query = self.sf.query_all(query)

        logging.info("Converting ordered dict to dict")
        query = json.loads(json.dumps(query))
        return query

    def describeObject(self, obj):
        """
        Get the description of an object from Salesforce
        """
        return json.loads(json.dumps(self.sf.__getattr__(obj).describe()))

    def convertSalesforceSchemaToBQ(self, obj, schema_filename=None, keys_to_use=None):
        """
        Given an object develop a schema for it.
        """
        desc = self.describeObject(obj)

        schema = [{
            "mode": "nullable" if d['nillable'] else "required",
            "name": d['name'],
            "type": self._typeMap(d['type'])
        } for d in desc['fields']]

        # we can shrink the resulting schema down
        if keys_to_use:
            schema = [s for s in schema if s['name'].lower() in keys_to_use]

        # if the filename is specified, dum p
        if schema_filename:
            with open(schema_filename, "w") as f:
                json.dump(schema, f)

        return schema

    @classmethod
    def _typeMap(cls, t):
        """
        Convert the SF type to a BQ type.

        If we don't have a mapping for the type, assume it's a string since almost everything goes to string.
        """
        cls.SF_TO_BQ_TYPE_MAP.get(t, "STRING")
     

    def writeQueryToFile(self, query_results, filename, fmt="csv"):
        """
        Write query results to file.

        Acceptable formats are csv, json

        This requires a significant amount of cleanup
        """
        fmt = fmt.lower()
        if fmt not in ['csv', 'json']:
            raise ValueError("Format value is not recognized: {0}".format(fmt))

        df = pd.DataFrame.from_records(query_results)            
        if 'attributes' in df.columns:
            del(df['attributes'])

        df.columns = [c.lower() for c in df.columns]

        # write the CSV file
        if fmt == "csv":
            df.to_csv(filename, index=False)
        elif fmt == "json":
            df.to_json(filename, "records")

        return df

