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

        logging.info("Received results: Total size: {0}; Done: {1}".format(query['totalSize'], query['done']))

        query = json.loads(json.dumps(query))
        return query

    def describeObject(self, obj):
        """
        Get the description of an object from Salesforce
        """
        return json.loads(json.dumps(self.sf.__getattr__(obj).describe()))

    def getAvailableFields(self, obj):
        """
        Get a list of all available fields for an object.

        This only returns the names of the fields.
        """
        desc = self.describeObject(obj)

        return [f['name'] for f in desc['fields']]

    def _buildFieldList(self, fields):
        # join all of the fields in a comma seperated list
        return ",".join(fields)

    def getObjectFromSalesforce(self, obj, fields):
        """
        Get all instances of the `object` from Salesforce.  For each model, only get the fields specified in fields.

        All we really do underneath the hood is run
            SELECT <fields> FROM <obj>
        """
        field_string = self._buildFieldList(fields)

        query = "SELECT {0} FROM {1}".format(field_string, obj)

        logging.info("Making query to salesforce: {0}".format(query))
        return self.makeQuery(query)

    def convertSalesforceSchemaToBQ(self, obj, fields, schema_filename=None):
        """
        Given an object develop a schema for it.
        """
        desc = self.describeObject(obj)

        schema = [{
            "mode": "nullable" if d['nillable'] else "required",
            "name": d['name'],
            "type": self._typeMap(d['type'])
        } for d in desc['fields']]

        # if the length of the fields array is less than the length of the object's complete schema,
        # then there are items that we need to remove
        if len(fields) < len(schema):
            schema = [s for s in schema if s['name'] in fields]

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
        return cls.SF_TO_BQ_TYPE_MAP.get(t, "STRING")

    def writeObjectToFile(self, query_results, filename, fmt="csv"):
        """
        Write query results to file.

        Acceptable formats are csv, json

        This requires a significant amount of cleanup
        """
        fmt = fmt.lower()
        if fmt not in ['csv', 'json']:
            raise ValueError("Format value is not recognized: {0}".format(fmt))

        df = pd.DataFrame.from_records(query_results)   

        # all of the results come with 
        if 'attributes' in df.columns:
            del(df['attributes'])

        df.columns = [c.lower() for c in df.columns]

        # write the CSV file
        if fmt == "csv":
            df.to_csv(filename, index=False)
        elif fmt == "json":
            df.to_json(filename, "records")

        return df

