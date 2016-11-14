from airflow.models import  BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
import json
import os

from airflow_salesforce.hooks.salesforce_hook import SalesforceHook

class SalesforceToFileOperator(BaseOperator):
    """
    Make a query against Salesforce
    """
    @apply_defaults
    def __init__ (
        self,
        conn_id,
        obj,
        output,
        fields = None,
        output_schemafile = None,
        output_schema = "BQ",
        fmt = "csv",
        record_time_added = False,
        coerce_to_timestamp = False,
        *args,
        **kwargs):
        """
        Download a Salesforce object's data to a file.  Optionally, you can also download the object's schema to a file as well.

        :param conn_id:             name of the Airflow connection that has your Salesforce username, password and security_token
        :param obj:                 name of the Salesforce object we are fetching data from
        :param output:              name of the file where the results should be saved
        :param fields:              *(optional)* list of fields that you want to get from the object. If *None*, then this will get all fields for the object
        :param output_schemafile:   *(optional)* name of the filre where the schema should be written to
        :param output_schema:       *(optional)* convert the Salesforce schema into a valid schema for a different database in JSON format.  This value should be the name of the other database as defined in `salesforce_hook`. *Default: BQ (BigQuery output)*
        :param fmt:                 *(optional)* format that the output of the data should be in.  *Default: CSV*
        :param record_time_added:   *(optional)* True if you want to add a Unix timestamp field to the resulting data that marks when the data was fetched from Salesforce. *Default: False*.
        :param coerce_to_timestamp: *(optional)* True if you want to convert all fields with dates and datetimes into Unix timestamp (UTC).  *Default: False*.
        """

        super(SalesforceToFileOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.output = output


        self.output_schemafile = output_schemafile
        self.output_schematype = output_schema
        self.object = obj
        self.fields = fields
        self.fmt = fmt.lower()
        self.record_time_added = record_time_added
        self.coerce_to_timestamp = coerce_to_timestamp

    def execute(self, context):
        """
        Execute the operator.  This will get all the data for a particular Salesforce model and write it to a file.  Optionally, this operator will also write the object's schema in JSON format to a file.
        """
        logging.info("Prepping to gather data from Salesforce")

        # load the SalesforceHook
        # this is what has all the logic for conencting and getting data from Salesforce
        hook = SalesforceHook(
            conn_id = self.conn_id,
            output = self.output
        )

        # attempt to login to Salesforce
        # if this process fails, it will raise an error and die right here
        # we could wrap it
        hook.signIn()

        # get object from salesforce
        # if fields were not defined, then we assume that the user wants to get all of them
        if not self.fields:
            self.fields = hook.getAvailableFields(self.object)
        logging.info("Making request for {0} fields from {1}".format(len(self.fields), self.object))

        query = hook.getObjectFromSalesforce(self.object, self.fields)


        # output the records from the query to a file
        # the list of records is stored under the "records" key
        logging.info("Writing query results to file: {0}".format(self.output))
        hook.writeObjectToFile(query['records'],
            filename =              self.output,
            fmt =                   self.fmt,
            coerce_to_timestamp =   self.coerce_to_timestamp,
            record_time_added =     self.record_time_added)

        if self.output_schemafile:
            logging.info("Writing schema to file: {0}".format(self.output_schemafile))
            hook.convertSalesforceSchemaToAnotherSchema(self.object, self.fields,
                other_schema =          self.output_schematype,
                schema_filename =       self.output_schemafile,
                coerce_to_timestamp =   self.coerce_to_timestamp,
                record_time_added =     self.record_time_added)

        logging.info("Query finished!")
