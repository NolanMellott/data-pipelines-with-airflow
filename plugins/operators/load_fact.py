from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                redshift_conn_id="redshift",
                table="" ,
                sql_statement="",
                append_data=False,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_data == False:
            self.log.info("Clearing data from {self.table} table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info("Inserting data into {self.table} fact table")
        redshift.run(f'INSERT INTO {self.table} {self.sql_statement}')
