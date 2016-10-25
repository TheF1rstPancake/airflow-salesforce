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
        fields,
        output = None,
        output_schema = None,
        query = None,
        *args,
        **kwargs):
        """
        Make a query against Salesforce
        """

        super(SalesforceToFileOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.output = output
        self.output_schema = output_schema
        self.object = obj
        self.fields = fields
        self.query = query

    def execute(self, context):
        logging.info("Prepping to gather data from Salesforce")
        hook = SalesforceHook(
            conn_id = self.conn_id,
            output = self.output
        )

        # get object from salesforce
        logging.info("Making request for {0} fields from {1}".format(len(self.fields), self.object))
        query = hook.getObjectFromSalesforce(self.object, self.fields)

        # if output is set, then output to file
        if self.output:
            # output the records from the query to a file
            # the list of records is stored under the "records" key
            logging.info("Writing query results to file: {0}".format(self.output))
            hook.writeObjectToFile(query['records'], filename = self.output, fmt="csv")

            if self.output_schema:
                logging.info("Writing schema to file: {0}".format(self.output_schema))
                hook.convertSalesforceSchemaToBQ(self.object, self.fields, self.output_schema)

        logging.info("Query finished!")

