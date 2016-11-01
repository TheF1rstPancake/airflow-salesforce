from airflow.models import  BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import json
import os

from salesforce_to_file.hooks.salesforce_hook import SalesforceHook

class SalesforceToFileOperator(BaseOperator):
    """
    Make a query against Salesforce
    """
    @apply_defaults
    def __init__ (
        self,
        conn_id,
        obj,
        fields = None,
        output = None,
        output_schemafile = None,
        output_schema = "BQ",
        query = None,
        fmt = "csv",
        coerce_to_timestamp = False,
        *args,
        **kwargs):
        """
        Make a query against Salesforce
        """

        super(SalesforceToFileOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.output = output
        self.output_schemafile = output_schemafile
        self.output_schematype = output_schema
        self.object = obj
        self.fields = fields
        self.query = query
        self.fmt = fmt.lower()
        self.coerce_to_timestamp = coerce_to_timestamp

    def execute(self, context):
        logging.info("Prepping to gather data from Salesforce")
        hook = SalesforceHook(
            conn_id = self.conn_id,
            output = self.output
        )

        # attempt to login to Salesforce
        hook.signIn()

        # get object from salesforce
        # if fields were not defined, then we assume that the user wants to get all of them
        if not self.fields:
            self.fields = hook.getAvailableFields(self.object)
        logging.info("Making request for {0} fields from {1}".format(len(self.fields), self.object))

        query = hook.getObjectFromSalesforce(self.object, self.fields)

        # if output is set, then output to file
        if self.output:
            # output the records from the query to a file
            # the list of records is stored under the "records" key
            logging.info("Writing query results to file: {0}".format(self.output))
            hook.writeObjectToFile(query['records'],
                filename =              self.output,
                fmt =                   self.fmt,
                coerce_to_timestamp =   self.coerce_to_timestamp)

            if self.output_schemafile:
                logging.info("Writing schema to file: {0}".format(self.output_schemafile))
                hook.convertSalesforceSchemaToAnotherSchema(self.object, self.fields,
                    other_schema =          self.output_schematype,
                    schema_filename =       self.output_schemafile,
                    coerce_to_timestamp =   self.coerce_to_timestamp)

        logging.info("Query finished!")
