from airflow.sdk import asset, Asset, Context

@asset(
    schedule="@daily",
    uri="https://randomuser.me/api/"  # Data source uri 
)
def user(self) -> dict[str]:
    import requests
    response = requests.get(self.uri)      # self.uri used beccause airflow 3.0 provide self with asset parameters. 
    return response.json()


# @asset(
#     schedule=user # this will get schedule from the user asset , user materializes user location materializes too. 
# )
# def user_location(user:Asset, context:Context) -> dict[str]:
#     user_data=context['ti'].xcom_pull(
#         dag_id=user.name, 
#         task_ids=user.name,
#         include_prior_dates=True
#     )
#     return user_data['results'][0]['location']


# @asset( schedule=user)
# def user_login(user,context):
#     user_data=context['ti'].xcom_pull(
#         dag_id=user.name, 
#         task_ids=user.name,
#         include_prior_dates=True
#     )
#     return user_data['results'][0]['login']


# instead of writing same code again and again we can use multi assest concept. 
@asset.multi(
    schedule=user,
    outlets=[
        Asset(name="user_location"),
        Asset(name="user_login")    
    ]
)
def user_info(user:Asset, context:Context):
    user_data=context['ti'].xcom_pull(
        dag_id=user.name, 
        task_ids=user.name,
        include_prior_dates=True
    )
    return [
        user_data['results'][0]['location'],
        user_data['results'][0]['login']
    ]




