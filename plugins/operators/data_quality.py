from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                redshift_conn_id="redshift",
                sql_statement="",
                expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.expected_result = expected_result

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Executing Query: {self.sql_statement}')
        #hook.get_first only allows checking the expected result of the *first* result of a query
        result = redshift.get_first(self.sql_statement)
        self.log.info(f'Query Result: {result}')

        if result[0] == self.expected_result:
            self.log.info('Data Quality Check Successful')
        else:
            raise AirflowFailException('Data Quality Check Unsuccessful')
