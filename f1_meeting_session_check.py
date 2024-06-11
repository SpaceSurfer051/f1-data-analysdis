from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import requests


# 문자열을 datetime 객체로 변환하는 함수
def parse_datetime(date_str, execution_flag = False):
    if execution_flag:
        if "." in date_str:
            # 소수점이 있는 경우
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f%z")
        else:
            # 소수점이 없는 경우
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S%z")
    else:
        if "." in date_str:
            # 소수점이 있는 경우
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f%z")
        else:
            # 소수점이 없는 경우
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
    


def get_meeting_and_session_from_open_f1(**kwargs):
    execution_date = parse_datetime(kwargs["execution_date"], True)
    left_search_window = (
        (execution_date - timedelta(days=3)).date().strftime("%Y-%m-%dT%H:%M:%S")
    )
    right_search_window = (
        (execution_date + timedelta(days=1)).date().strftime("%Y-%m-%dT%H:%M:%S")
    )
    print(
        "left_search_window: ", left_search_window,
        "right_search_window: ", right_search_window,
    )

    last_meeting = requests.get(
        f"https://api.openf1.org/v1/meetings?date_start>{left_search_window}&date_start<{right_search_window}"
    ).json()

    if len(last_meeting) > 0:
        meeting_key = last_meeting[-1]["meeting_key"]
        session = requests.get(
            f"https://api.openf1.org/v1/sessions?meeting_key={meeting_key}"
        ).json()

        if session[-1]["session_type"] in ["Qualifying", "Race"]:
            session_date = parse_datetime(session[-1]["date_start"], False)

            if session_date.date() == execution_date.date():
                print("new Race is hold!! session date:", session_date.date())

                session_key = session[-1]["session_key"]
                kwargs["ti"].xcom_push(key="session_key", value=session_key)
                kwargs["ti"].xcom_push(key="meeting_key", value=meeting_key)
                return True

    print("new Race is not hold...")
    return False


def check_condition(**kwargs):
    new_data_flag = kwargs["ti"].xcom_pull(task_ids="get_meeting_and_session")
    if new_data_flag:
        return "trigger_laps_dag"
    else:
        return "skip_trigger"


with DAG(
    dag_id="f1_meeting_session_check",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule="0 0 * * *",
) as dag:

    get_meeting_and_session = PythonOperator(
        task_id="get_meeting_and_session",
        python_callable=get_meeting_and_session_from_open_f1,
        op_kwargs={"execution_date": "{{ execution_date }}"},
    )

    check_condition_to_trigger = BranchPythonOperator(
        task_id="check_condition_to_trigger", python_callable=check_condition
    )

    trigger_laps_dag = TriggerDagRunOperator(
        task_id="trigger_laps_dag",
        trigger_dag_id="f1_laps_data_pipeline",
        conf={
            "session_key": "{{ task_instance.xcom_pull(task_ids='get_meeting_and_session', key='session_key') }}",
            "meeting_key": "{{ task_instance.xcom_pull(task_ids='get_meeting_and_session', key='meeting_key') }}",
        },
    )
    trigger_session_dag = TriggerDagRunOperator(
        task_id="trigger_session_dag",
        trigger_dag_id="f1_session_data_pipeline"
    )
    
    pit_trigger = TriggerDagRunOperator(
        task_id="pit_trigger",
        trigger_dag_id="f1_pit_stop_data_pipeline"
    )
    
    meetings_trigger = TriggerDagRunOperator(
        task_id="meetings_trigger",
        trigger_dag_id="f1_meetings_data_pipeline"
    )
    
    position_trigger = TriggerDagRunOperator(
        task_id="position_trigger",
        trigger_dag_id="f1_position_data_pipeline",
        conf={
            "session_key": "{{ task_instance.xcom_pull(task_ids='get_meeting_and_session', key='session_key') }}",
            "meeting_key": "{{ task_instance.xcom_pull(task_ids='get_meeting_and_session', key='meeting_key') }}",
        },
    )

    skip = DummyOperator(
        task_id="skip_trigger",
        dag=dag,
    )

    get_meeting_and_session >> check_condition_to_trigger
    check_condition_to_trigger >> trigger_laps_dag >> trigger_session_dag
    check_condition_to_trigger >> pit_trigger
    check_condition_to_trigger >> meetings_trigger
    check_condition_to_trigger >> position_trigger
    check_condition_to_trigger >> skip
