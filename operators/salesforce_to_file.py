from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging
import json

class SalesforceToFileOperator(BaseOperator):
    """
    Make a query against Salesforce and
    write the resulting data to a file.
    """
    template_fields = ("query", )

    @apply_defaults
    def __init__(
        self,
        conn_id,
        obj,
        output,
        fields=None,
        fmt="csv",
        query=None,
        relationship_object=None,
        record_time_added=False,
        coerce_to_timestamp=False,
        *args,
        **kwargs
    ):
        """
        Initialize the operator

        :param conn_id:             name of the Airflow connection that has
                                    your Salesforce username, password and
                                    security_token
        :param obj:                 name of the Salesforce object we are
                                    fetching data from
        :param output:              name of the file where the results
                                    should be saved
        :param fields:              *(optional)* list of fields that you want
                                    to get from the object.
                                    If *None*, then this will get all fields
                                    for the object
        :param fmt:                 *(optional)* format that the output of the
                                    data should be in.
                                    *Default: CSV*
        :param query:               *(optional)* A specific query to run for
                                    the given object.  This will override
                                    default query creation.
                                    *Default: None*
        :param relationship_object: *(optional)* Some queries require
                                    relationship objects to work, and
                                    these are not the same names as
                                    the SF object.  Specify that
                                    relationship object here.
                                    *Default: None*
        :param record_time_added:   *(optional)* True if you want to add a
                                    Unix timestamp field to the resulting data
                                    that marks when the data was
                                    fetched from Salesforce.
                                    *Default: False*.
        :param coerce_to_timestamp: *(optional)* True if you want to convert
                                    all fields with dates and datetimes
                                    into Unix timestamp (UTC).
                                    *Default: False*.
        """

        super(SalesforceToFileOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.output = output

        self.object = obj
        self.fields = fields
        self.fmt = fmt.lower()
        self.query = query
        self.relationship_object = relationship_object
        self.record_time_added = record_time_added
        self.coerce_to_timestamp = coerce_to_timestamp

    def special_query(self, query, sf_hook, relationship_object=None):
        if not query:
            raise ValueError("Query is None.  Cannot query nothing")

        sf_hook.sign_in()

        results = sf_hook.make_query(query)
        if relationship_object:
            records = []
            for r in results['records']:
                if r.get(relationship_object, None):
                    records.extend(r[relationship_object]['records'])
            results['records'] = records

        return results

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Salesforce model
        and write it to a file.

        """
        logging.info("Prepping to gather data from Salesforce")

        # load the SalesforceHook
        # this is what has all the logic for
        # conencting and getting data from Salesforce
        hook = SalesforceHook(
            conn_id=self.conn_id,
            output=self.output
        )

        # attempt to login to Salesforce
        # if this process fails, it will raise an error and die right here
        # we could wrap it
        hook.sign_in()

        # get object from salesforce
        # if fields were not defined,
        # then we assume that the user wants to get all of them
        if not self.fields:
            self.fields = hook.get_available_fields(self.object)

        logging.info(
            "Making request for"
            "{0} fields from {1}".format(len(self.fields), self.object)
        )

        if self.query:
            query = self.special_query(
                self.query,
                hook,
                relationship_object=self.relationship_object
            )
        else:
            query = hook.get_object_from_salesforce(self.object, self.fields)

        # output the records from the query to a file
        # the list of records is stored under the "records" key
        logging.info("Writing query results to: {0}".format(self.output))
        hook.write_object_to_file(
            query['records'],
            filename=self.output,
            fmt=self.fmt,
            coerce_to_timestamp=self.coerce_to_timestamp,
            record_time_added=self.record_time_added
        )

        logging.info("Query finished!")
