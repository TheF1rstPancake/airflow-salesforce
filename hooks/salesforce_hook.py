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
        "date":         "STRING",
        "datetime":     "FLOAT",    # we convert all datetime values to UNIX timestamps, which are floats
        "currency":     "FLOAT",
        "double":       "FLOAT",
        "int":          "FLOAT",
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
            conn_id,
            *args,
            **kwargs
        ):
        """
        Create new connection to Salesforce

        :param conn_id:     the name of the connection that has the parameters we need to connect to Salesforce.  The conenction shoud be type `http` and include a user's security token in the `Extras` field.

        For the HTTP connection type, you can include a JSON structure in the `Extras` field.  We need a user's security token to connect to Salesforce.  So we define it in the `Extras` field as: `{"security_token":"YOUR_SECRUITY_TOKEN"}`
        """
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        # get the connection parameters
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def signIn(self):
        """
        Sign into Salesforce.  If we have already signed it, this will just return the original object
        """
        if hasattr(self, 'sf'):
            return self.sf

        # connect to Salesforce
        sf = Salesforce(username=self.connection.login, password=self.connection.password, security_token=self.extras['security_token'], instance_url=self.connection.host)
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
        if not hasattr(self, 'sf'):
            self.signIn()

        return json.loads(json.dumps(self.sf.__getattr__(obj).describe()))

    def getAvailableFields(self, obj):
        """
        Get a list of all available fields for an object.

        This only returns the names of the fields.
        """
        if not hasattr(self, 'sf'):
            self.signIn()

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
        logging.info("Making query to salesforce: {0}".format(query if len(query)<30 else " ... ".join([query[:15], query[-15:]])))
        return self.makeQuery(query)

    def convertSalesforceSchemaToBQ(self, obj, fields, schema_filename=None):
        """
        Given an object develop a schema for it.
        """
        desc = self.describeObject(obj)

        schema = [{
            "mode": "NULLABLE" if d['nillable'] else "REQUIRED",
            "name": d['name'],
            "type": self._typeMap(d['type'])
        } for d in desc['fields']]

        # the schema objects have to be listed in the same order as they appear in the CSV file
        # the dataframe has it's columns sorted alphabetically, so we sort the schema the same way
        schema = sorted(schema, key = lambda x: x['name'])

        # if the length of the fields array is less than the length of the object's complete schema,
        # then there are items that we need to remove
        if len(fields) < len(schema):
            schema = [s for s in schema if s['name'] in fields]

        # if the filename is specified, dump
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

    @classmethod
    def _toTimestamp(cls, col):
        """
        Convert a column of a dataframe to UNIX timestamps if applicable
        """
        # try and convert the column to datetimes
        # the column MUST have a four digit year somewhere in the string
        # there should be a better way to do this,
        # but just letting pandas try and convert every column without a format caused it to convert floats as well
        # For example, a column of integers between 0 and 10 are turned into timestamps
        # if the column cannot be converted, just return the original column untouched
        try:
            col = pd.to_datetime(col)
        except ValueError as e:
            return col

        # now convert the newly created datetimes into timestamps
        # we have to be careful here because NaT cannot be converted to a timestamp
        # so we have to return NaN
        converted = []
        for i in col:
            try:
                converted.append(i.timestamp())
            except ValueError as e:
                converted.append(pd.np.NaN)

        # return a new series that maintains the same index as the original
        return pd.Series(converted, index= col.index)

    def writeObjectToFile(self, query_results, filename, fmt="csv"):
        """
        Write query results to file.

        Acceptable formats are csv, json

        This requires a significant amount of cleanup
        """
        fmt = fmt.lower()
        if fmt not in ['csv', 'json', 'ndjson']:
            raise ValueError("Format value is not recognized: {0}".format(fmt))

        # this line right here will convert all integers to floats if there are any None/np.nan values in the column
        # that's because None/np.nan cannot exist in an integer column
        # but we write all integers as FLOATS in our BQ schema, so it's fine
        df = pd.DataFrame.from_records(query_results, exclude=["attributes"])

        df.columns = [c.lower() for c in df.columns]

        # convert columns with datetime strings to datetimes
        # not all strings will be datetimes, so we ignore any errors that occur
        possible_timestamp_cols = df.columns[df.dtypes == "object"]
        df[possible_timestamp_cols] = df[possible_timestamp_cols].apply(lambda x: self._toTimestamp(x))


        # write the CSV or JSON file depending on the option
        # NOTE:
        #   datetimes here are an issue.  There is no good way to manage the difference
        #   for to_json, the options are an epoch or a ISO string
        #   but for to_csv, it will be a string output by datetime
        #   For JSON we decided to output the epoch timestamp in seconds (as is fairly standard for JavaScript)
        #   And for csv, we do a string
        if fmt == "csv":
            # there are also a ton of newline objects that mess up our ability to write to csv
            # we remove these newlines so that the output is a valid CSV format
            logging.info("Cleaning data and writing to CSV")
            possible_strings = df.columns[df.dtypes == "object"]
            df[possible_strings] = df[possible_strings].apply(lambda x: x.str.replace("\r\n", ""))
            df[possible_strings] = df[possible_strings].apply(lambda x: x.str.replace("\n", ""))

            # write the dataframe
            df.to_csv(filename, index=False)
        elif fmt == "json":
            df.to_json(filename, "records", date_unit="s")
        elif fmt =="ndjson":
            df.to_json(filename, "records", lines=True, date_unit="s")

        return df
