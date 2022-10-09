from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def get_driver(options=None, driver_path=r'D:\servers\chromedriver_win32\chromedriver.exe'):
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def open_chrome(driver, url):
    driver.get(url)


def get_html(driver):
    return driver.execute_script("return document.documentElement.outerHTML")


def close(driver):
    if driver:
        driver.close()
    if driver:
        driver.quit()
