import time

from selenium import webdriver
from selenium.common import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from sqlalchemy import update, text
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

from Querys import entidades, warehouse

options = Options()
options.add_experimental_option("detach", True)

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)


def tramites_consultas(driver):
    # Assuming you have already initialized the driver
    o_window = driver.current_window_handle

    # Navigate to the webpage
    driver.get("https://www.sunat.gob.pe/sol.html")

    # Maximize the window
    driver.maximize_window()

    # Set implicit wait
    driver.implicitly_wait(1.5)  # 1500 milliseconds converted to seconds

    # Click on "Trámites y Consultas"
    driver.find_element(By.XPATH, "/html/body/section[1]/div/div/section[2]/div[2]/div/a/span").click()

    # Get all window handles
    all_window_handles = driver.window_handles

    # Find the first popup handle
    first_popup_handle = None
    for handle in all_window_handles:
        if handle != o_window:
            first_popup_handle = handle
            break

    # Switch to the first popup
    driver.switch_to.window(first_popup_handle)

    # Wait for the second popup to appear (you can adjust this time)
    time.sleep(2)  # Or use WebDriverWait for a more robust solution

    # Get window handles again to find the second popup
    all_window_handles = driver.window_handles

    # Find the second popup handle
    second_popup_handle = None
    for handle in all_window_handles:
        if handle != o_window and handle != first_popup_handle:
            second_popup_handle = handle
            break

    if second_popup_handle:
        # Switch back to the first popup to close it
        driver.switch_to.window(first_popup_handle)

        # Close the first popup
        driver.close()

        # Switch to the second popup
        driver.switch_to.window(second_popup_handle)

        driver.maximize_window()

    return driver


def login_tramites_consultas(driver, credenciales):
    time.sleep(2)

    driver.find_element(By.ID, "txtRuc").send_keys(credenciales[0])  # ingresar ruc
    driver.find_element(By.ID, "txtUsuario", ).send_keys(credenciales[1])  # ingresar usuario
    driver.find_element(By.ID, "txtContrasena").send_keys(credenciales[2])  # ingresar contrasena
    driver.find_element(By.ID, "btnAceptar").click()  # aceptar
    print(credenciales[0])
    wait = WebDriverWait(driver, 3)  # Timeout in seconds
    print(1)
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "ifrVCE")))
    print(2)
    try:
        # Look for the modal dialog
        print(3)
        wait.until(EC.element_to_be_clickable((By.ID, "btnFinalizarValidacionDatos")))
        print(4)
        temp_dialog = driver.find_element(By.CLASS_NAME, "modal-dialog")
        print(5)
        temp_dialog.find_element(By.ID, "btnFinalizarValidacionDatos").click()
        print(6)
        driver.find_element(By.ID, "btnCerrar").click()
        print("VALIDACION DE CONTACTO PENDIENTE")
    except TimeoutException:
        print("No hay dialogo de validacion")
        # Check for mailbox message (second element)
        try:
            print("Esperando mensaje de buzon...")
            driver.find_element(By.ID, "btnCerrar").click()  # Adjust selector if necessary
            print("BUZON POR REVISAR")
        except TimeoutException:
            print("No hay mensaje de buzon")

    finally:
        driver.switch_to.default_content()
        # Optionally, you might want to click on some service option here,
        # but it's commented out in your Java code
        #driver.find_element(By.ID, "divOpcionServicio2").click()
        print("AUTENTICACION CORRECTA")
    return driver



def reporte_tregistro(driver, reporte: str = 'IDE'):
    print(1)
    valores = {
        'DIR': '5',
        'TRA': '6',
        'SSA': '7',
        'PEN': '8',
        'PFL': '9',
        'TER': '10',
        'SEP': '15',
        'SET': '14',
        'RPI': '16',
        'AST': '17',
        'ERROR': '11'
    }
    driver.find_element(By.ID, "divOpcionServicio2").click() #Empresas
    print(1)
    driver.find_element(By.ID, "nivel1_10").click()  #Mi RUC y otros registros
    print(1)
    driver.find_element(By.ID, "nivel2_10_5").click()  #T-Registro
    print(1)
    driver.find_element(By.ID, "nivel3_10_5_3").click()  #Registro de Trabaj., Pension., Pers. en forma
    print(1)
    driver.find_element(By.ID, "nivel4_10_5_3_1_3").click()  #Consultas y reportes
    print(1)
    wait = WebDriverWait(driver, 3)  # Timeout in seconds
    print(1)
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "iframeApplication")))
    print(1)
    driver.find_element(By.ID, "adescarga").click()  #Descarga de Información del Prestador de Servicios
    print(1)
    select = Select(driver.find_element(By.ID, 'selTipDes'))  #Lista de reportes a elegir para descargar
    print(1)
    select.select_by_value(valores.get(reporte))
    print(1)
    driver.find_element(By.ID, "btnRegistrar").click() #Registrar el pedido de descarga
    print(1)
    wait.until(
        EC.presence_of_element_located((By.ID, "dlgPanel_XX1"))
    )
    button = driver.find_element(By.CSS_SELECTOR, ".btn.btn-success.btn-ok")
    print(1)
    button.click()

#EJECUCION DEL FLUJO
new_driver = tramites_consultas(driver=driver)
provisional = [['20613333593', '45676334', 'Julito2024@'], ]
entidades['ruc'] = entidades['ruc'].astype(str)
entidades = entidades[(entidades['activo']) &
                      (~entidades['observaciones'].fillna('').str.contains('FALLA AUTENTICA', case=False))]
if any(any(sublista) for sublista in provisional):
    credenciales = provisional
else:
    credenciales = entidades[['ruc', 'usuario_sol', 'clave_sol']].values.tolist()

for credencial in credenciales:
    try:
        driver = login_tramites_consultas(new_driver, credencial)


    except Exception as e:
        print('ERROR EN EL LOGIN')
        if "falla" in driver.find_element(By.CLASS_NAME, "col-md-12").text.lower():
            driver.find_element(By.ID, "btnVolver").click()
            with warehouse.connect() as connection:
                query = text(
                    "UPDATE priv.entities SET observaciones = CONCAT(observaciones, '|FALLA AUTENTICACION') WHERE ruc = :ruc")
                connection.execute(query, {"ruc": str(credencial[0])})
                connection.commit()
            print('FALLA DE AUTENTICACION REGISTRADA')
    finally:
        driver.find_element(By.ID, "btnSalir").click()





"""

id="adescarga" 
id="selTipDes"	
<option value="4">TR3: Datos de identificación (IDE)</option>
<option value="5">TR4: Dirección (DIR)</option>
<option value="6">TR5: Datos laborales del trabajador (TRA)</option>
<option value="7">TR6: Datos de seguridad social y adicionales (SSA)</option>
<option value="8">TR7: Pensionistas (PEN)</option>
<option value="9">TR8: Personal en formación (PFL)</option>
<option value="10">TR9: Personal de terceros (TER)</option>
<option value="15">TR10: Datos de la situación educativa - Pendientes (SEP)</option>
<option value="14">TR11: Datos de la situación educativa - Todos (SET)</option>
<option value="16">TR12: Datos de trabajadores con inconsistencias en el Régimen Pensionario (RPI)</option>
<option value="17">TR13: Datos de la última afiliación sindical de cada trabajador (AST)</option>
<option value="11">Registros inconsistentes y/o incompletos</option>

id="btnRegistrar" 

id="dlgPanel_XX1"	MODAL QUE APARECE AL REGISTRAR EL PEDIDO
class="btn btn-success btn-ok" BOTTON ACEPTAR
//*[@id="dlgPanel_XX1"]/div[2]/div/div[2]/button XPATH DEL BOTON


id="nivel1_12" Mis declaraciones informativas
id="nivel2_12_8" Consulto mis declaraciones y pagos
id="nivel3_12_8_1" Declaraciones y pagos
id="nivel4_12_8_1_1_2" Consulta general
id="iframeApplication" FRAME


id="nivel4_12_8_1_1_4" Consulta y descarga de pedidos de declaracion"""
