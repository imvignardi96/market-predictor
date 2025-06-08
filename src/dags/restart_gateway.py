import pendulum
import logging
from airflow.decorators import task, dag
from airflow.exceptions import AirflowSkipException, AirflowFailException


# Define your DAG using the @dag decorator
@dag(
    dag_id='check_gateway',
    description='DAG para reinniciar servicio gateway',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    schedule='30 22 * * *',  # At 22:30
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=2),
    },
    doc_md=
    """
        #### Documentacion Iniciador Gateway.
        
        El presente DAG se encarga de lanzar/reiniciar el Gateway de Interactive Brokers.
        
        Para poder utilizar la API se necesita este software instalado y accesible. 
        Por defecto solo se puede acceder a traves de localhost. Luego esta instalado en la propia maquina de airflow.
        La documentacion de la API indica que el programa tiene que ser reiniciado diariamente.
        
        Los pasos que realiza son:
        1) Check si el software esta ejecutando. Si ejecuta se cierra la instancia.
        2) Iniciar el software.
    """
)
def check_gateway_dag():

    @task(
        doc_md=
        """
            Esta tarea chequea si el gateway esta ejecutando y lo cierra en caso afirmativo.
        """
    )
    def check_program():
        import psutil
        for process in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            try:
                # Check if the process has a valid cmdline
                cmdline = process.info.get('cmdline', [])
                
                # Join only if the cmdline is not empty or None
                if cmdline and 'ibgateway' in ' '.join(cmdline).lower():
                    logging.info(f"Proceso localizado: {' '.join(cmdline)} - {process.info['pid']}")
                    process.terminate()  # Gracefully terminate the process
                    process.wait()  # Wait for process to terminate
                    logging.info(f"Proceso con ID {process.info['pid']} finalizado.")
                    return
            except psutil.NoSuchProcess:
                logging.info("Proceso no existente.")
                raise AirflowSkipException
            except psutil.AccessDenied:
                logging.warning(f"Acceso denegado a {process.info['pid']}")
                raise AirflowSkipException
            except psutil.ZombieProcess:
                logging.warning(f"Proceso {process.info['pid']} es un proceso zombie.")
                raise AirflowSkipException
            except Exception as e:
                logging.error(f"Error: {e}")
                raise AirflowFailException
        
        logging.info("Proceso no localizado.")

    @task(
        doc_md=
        """
            Esta tarea inicia el gateway
        """,
        trigger_rule = 'all_done'
    )
    def start_program():
        import subprocess
        import pyautogui  # If pyautogui works fine for your Linux setup
        import time
        import os
        from airflow.models.variable import Variable

        try:
            ib_user = Variable.get('ib_user_secret')
            ib_password = Variable.get('ib_password_secret')

            # Path to the IB Gateway executable on Linux
            ib_gateway_command = [
                "/home/ignacio/.local/share/i4j_jres/Oda-jK0QgTEmVssfllLP/1.8.0_202_64/bin/java",
                "-splash:/home/ignacio/Jts/ibgateway/1033/.install4j/s_clyey9.png",
                "-DinstallDir=/home/ignacio/Jts/ibgateway/1033/",
                "-Djava.library.path=/home/ignacio/Jts/ibgateway/1033",
                "-cp", "/home/ignacio/Jts/ibgateway/1033/jars/*",
                "install4j.ibgateway.GWClient"
            ]
            base_path = '/home/ignacio/airflow/market-predictor/src/images'

            # Start IB Gateway using subprocess
            subprocess.Popen(ib_gateway_command)
            
            logging.info('Proceso iniciado')

            time.sleep(10)

            try:
                api_button_location = pyautogui.locateCenterOnScreen(os.path.join(base_path, 'ib_api_button.png'))
                x, y = api_button_location
                pyautogui.click(x, y)

                time.sleep(2)

                paper_button_location = pyautogui.locateCenterOnScreen(os.path.join(base_path, 'paper_button.png'))
                x, y = paper_button_location
                pyautogui.click(x, y)

                time.sleep(2)
            except:
                logging.info('Opcion ya seleccionada')
                
            pyautogui.write(ib_user)

            pwd_field_location = pyautogui.locateCenterOnScreen(os.path.join(base_path, 'password_field.png'), confidence=0.9)
            x, y = pwd_field_location
            pyautogui.click(x, y)
            pyautogui.write(ib_password)

            log_button_location = pyautogui.locateCenterOnScreen(os.path.join(base_path, 'login_button.png'), confidence=0.8)
            x, y = log_button_location
            pyautogui.click(x, y)

            time.sleep(20)

            try:
                cookies_button_location = pyautogui.locateCenterOnScreen(os.path.join(base_path, 'cookies_button.png'), confidence=0.8)
                x, y = cookies_button_location
                pyautogui.click(x, y)
            except:
                logging.info('Cookies ya aceptadas.')

            time.sleep(2)

            try:
                close_button_location = pyautogui.locateCenterOnScreen(os.path.join(base_path, 'close_crap.png'), confidence=0.9)
                x, y = close_button_location
                pyautogui.click(x, y)
            except:
                logging.info('Ventana no estaba abierta.')

            time.sleep(1)

            accept_button_location = pyautogui.locateCenterOnScreen(os.path.join(base_path, 'accept_button.png'), confidence=0.9)
            x, y = accept_button_location
            pyautogui.click(x, y)
            
        except Exception as e:
            logging.error(f'Fallo al iniciar IB Gateway: {str(e)}')
            raise AirflowFailException
            
        logging.info('Login realizado')
        
    # Workflow logic: Check programa ejecutando. Si ejecuta, reiniciar.
    check = check_program()
    start = start_program()
    
    check >> start


# Create the DAG instance
check_gateway_instance = check_gateway_dag()