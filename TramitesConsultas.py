import time

from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

options = Options()
options.add_experimental_option("detach", True)

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

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

# Maximize window again (although it might already be maximized)
driver.maximize_window()
#-----------------------------------------------------

time.sleep(2)

driver.find_element(By.ID, "txtRuc").send_keys('20606283858')  # ingresar ruc
driver.find_element(By.ID, "txtUsuario", ).send_keys('TONERTAT')  # ingresar usuario
driver.find_element(By.ID, "txtContrasena").send_keys('rcavinsio')  # ingresar contrasena
driver.find_element(By.ID, "btnAceptar").click()  # aceptar

try:
    print("Tercer try")
    wait = WebDriverWait(driver, 3)  # Timeout in seconds
    wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "ifrVCE")))

    try:
        # Look for the modal dialog
        temp_dialog = driver.find_element(By.CLASS_NAME, "modal-dialog")
        temp_dialog.find_element(By.ID, "btnFinalizarValidacionDatos").click()
        driver.find_element(By.ID, "btnCerrar").click()
        driver.switch_to.default_content()  # Switch back to the parent frame
        print("No fallo segundo try")
    except Exception as e:
        print("Primer catch")
        driver.find_element(By.ID, "btnCerrar").click()
        driver.switch_to.default_content()
        print("No fallo primer catch")
except TimeoutException:
    # If the frame is not available within the timeout, switch back to parent frame
    driver.switch_to.default_content()
    print("Timeout exception occurred")
except Exception as e:
    driver.switch_to.default_content()
    # Optionally, you might want to click on some service option here,
    # but it's commented out in your Java code
    # driver.find_element(By.ID, "divOpcionServicio2").click()
    print("General exception occurred")

# Note: You need to define or import `set_credentials` function and `Entities` class or object for this to work
