from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag
def user_processing():
    create_table=SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        create table if not exists users(
        id int primary key,
        firstname varchar(255),
        lastname varchar(255),
        email varchar(255)
        )
        """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue :
        import requests 
        response=requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code==200:
            condition=True
            fake_user=response.json()
        else : 
            condition=False
            fake_user=None
        return PokeReturnValue(is_done=condition , xcom_value=fake_user)

    @task
    def extract_user(fake_user):
        # -----------for demo--------------- because in test mode xcom do not passes value.---------------
        # import requests 
        # response=requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        # fake_user=response.json
        #-------------------------------------------------------------------------------------------------

        return {
            "id":fake_user["id"],
            "firstname" : fake_user["personalInfo"]["firstName"],
            "lastname" : fake_user["personalInfo"]["lastName"], 
            "email" : fake_user["personalInfo"]["email"]
        }

    @task
    def process_user(user_info):
        import csv

        # user_info={
        #     "id":"123",
        #     "firstname" : "himanshu",
        #     "lastname" : "sachan", 
        #     "email" : "hs5050407@gmail.com"
        # }

        with open("/tmp/user_info.csv","w", newline="") as f:
            writer=csv.DictWriter(f,fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook=PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="copy users from stdin with csv header",
            filename="/tmp/user_info.csv"
        )


    api_available = is_api_available()
    user = extract_user(api_available)
    processed_user = process_user(user)
    stored_user = store_user()

    create_table >> api_available >> user >> processed_user >> stored_user

user_processing()