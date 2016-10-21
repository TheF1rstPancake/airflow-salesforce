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
        username,
        password,
        obj,
        fields,
        security_token = None,
        output = None,
        query = None,
        *args,
        **kwargs):
        """
        Make a query against Salesforce
        """

        super(SalesforceToFileOperator, self).__init__(*args, **kwargs)

        self.username = username
        self.password = password
        self.security_token = security_token
        self.output = output
        self.object = obj
        self.fields = fields
        self.query = query

    def execute(self, context):
        logging.info("Making query: {0}".format(self.query))

        hook = SalesforceHook(
            self.username, 
            self.password, 
            security_token=self.security_token, 
            output = self.output
        )

        query = hook.makeQuery(self.query)

        if self.output:
            # output the records from the query to a file
            logging.info("Writing query results to file: {0}".format(self.output))
            cols = hook.writeQueryToFile(query['records'], self.output)
            cols = list(cols.columns)

            # generate a schema file
            file_parts = os.path.splitext(self.output)
            schema_file = "{0}_schema.json".format(file_parts[0])

            hook.convertSalesforceSchemaToBQ(self.object, schema_file, cols)

        logging.info("Query finished!")

