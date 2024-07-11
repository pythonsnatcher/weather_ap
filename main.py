import os
import time
import requests
from datetime import datetime
import pandas as pd
from lxml import html
import mysql.connector

# Function to map pollution and UV index codes to descriptive levels
def map_level(code):
    if code == 'L':
        return 'Low'
    elif code == 'M':
        return 'Medium'
    elif code == 'H':
        return 'High'
    else:
        return 'Unknown'

# Function to scrape tide times
def scrape_tide_times():
    url = 'https://www.bbc.co.uk/weather/coast-and-sea/tide-tables/2/113'
    response = requests.get(url)
    tree = html.fromstring(response.content)
    
    tide_times = []
    
    low_tide_xpath_morning = '//*[@id="section-2024-07-11"]/table/tbody/tr[1]/td[1]/span'
    low_tide_xpath_evening = '//*[@id="section-2024-07-11"]/table/tbody/tr[2]/td[1]/span'
    high_tide_xpath_morning = '//*[@id="section-2024-07-11"]/table/tbody/tr[3]/td[1]/span'
    high_tide_xpath_evening = '//*[@id="section-2024-07-11"]/table/tbody/tr[4]/td[1]/span'

    low_tide_elem_morning = tree.xpath(low_tide_xpath_morning)
    low_tide_elem_evening = tree.xpath(low_tide_xpath_evening)
    high_tide_elem_morning = tree.xpath(high_tide_xpath_morning)
    high_tide_elem_evening = tree.xpath(high_tide_xpath_evening)

    low_tide_time_morning = low_tide_elem_morning[0].text.strip() if low_tide_elem_morning else "N/A"
    low_tide_time_evening = low_tide_elem_evening[0].text.strip() if low_tide_elem_evening else "N/A"
    high_tide_time_morning = high_tide_elem_morning[0].text.strip() if high_tide_elem_morning else "N/A"
    high_tide_time_evening = high_tide_elem_evening[0].text.strip() if high_tide_elem_evening else "N/A"

    tide_times.append((low_tide_time_morning, low_tide_time_evening))
    tide_times.append((high_tide_time_morning, high_tide_time_evening))

    return tide_times

# Function to scrape weather data
def get_weather_data():
    url = 'https://www.bbc.com/weather/2643743'
    response = requests.get(url)
    tree = html.fromstring(response.content)
    
    location_xpath = '//*[@id="wr-location-name-id"]'
    location_elem = tree.xpath(location_xpath)
    location = location_elem[0].text.strip() if location_elem else "N/A"

    high_temp_xpath = '//*[@id="daylink-0"]/div[4]/div[1]/div/div[4]/div/div[1]/span[2]/span/span[1]'
    high_temperature_elem = tree.xpath(high_temp_xpath)
    high_temperature = high_temperature_elem[0].text.strip() if high_temperature_elem else "N/A"

    low_temp_xpath = '//*[@id="daylink-0"]/div[4]/div[1]/div/div[4]/div/div[2]/span[2]/span/span[1]'
    low_temperature_elem = tree.xpath(low_temp_xpath)
    low_temperature = low_temperature_elem[0].text.strip() if low_temperature_elem else "N/A"

    current_temp_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[1]/div[2]/div[3]/div[2]/div/div/div[2]/span/span[1]'
    current_temperature_elem = tree.xpath(current_temp_xpath)
    current_temperature = current_temperature_elem[0].text.strip() if current_temperature_elem else "N/A"

    weather_condition_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[2]/div/span'
    weather_condition_elem = tree.xpath(weather_condition_xpath)
    weather_condition = weather_condition_elem[0].text.strip() if weather_condition_elem else "N/A"

    pollution_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[4]/div/div[1]/div[2]/span[3]/span[1]/span[2]'
    pollution_elem = tree.xpath(pollution_xpath)
    pollution_level = pollution_elem[0].text_content().strip() if pollution_elem else "N/A"
    pollution_level = map_level(pollution_level)

    chance_of_precipitation_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[1]/div[2]/div[3]/div[3]/div[2]'
    chance_of_precipitation_elem = tree.xpath(chance_of_precipitation_xpath)
    chance_of_precipitation = chance_of_precipitation_elem[0].text.strip() if chance_of_precipitation_elem else "N/A"

    pollen_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[4]/div/div[1]/div[2]/span[1]/span[1]/span[2]'
    pollen_elem = tree.xpath(pollen_xpath)
    pollen_level = pollen_elem[0].text_content().strip() if pollen_elem else "N/A"
    pollen_level = map_level(pollen_level)

    wind_speed_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[1]/div[2]/div[3]/div[4]/div/span[3]/span/span[1]'
    wind_speed_elem = tree.xpath(wind_speed_xpath)
    wind_speed = wind_speed_elem[0].text.strip() if wind_speed_elem else "N/A"

    humidity_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[2]/div/div/div[1]/dl/dd[1]'
    humidity_elem = tree.xpath(humidity_xpath)
    humidity = humidity_elem[0].text.strip() if humidity_elem else "N/A"

    wind_direction_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[2]/div/div/div[4]'
    wind_direction_elem = tree.xpath(wind_direction_xpath)
    wind_direction = wind_direction_elem[0].text_content().strip() if wind_direction_elem else "N/A"

    pressure_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[2]/div/div/div[1]/dl/dd[2]'
    pressure_elem = tree.xpath(pressure_xpath)
    pressure = pressure_elem[0].text.strip() if pressure_elem else "N/A"

    visibility_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[2]/div/div/div/div[2]/ol/li[1]/button/div[2]/div/div/div[1]/dl/dd[3]'
    visibility_elem = tree.xpath(visibility_xpath)
    visibility = visibility_elem[0].text.strip() if visibility_elem else "N/A"

    uv_index_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[4]/div/div[1]/div[2]/span[2]/span[1]/span[2]'
    uv_index_elem = tree.xpath(uv_index_xpath)
    uv_index = uv_index_elem[0].text.strip() if uv_index_elem else "N/A"
    uv_index = map_level(uv_index)

    sunrise_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[4]/div/div[1]/div[1]/span[1]/span[2]'
    sunrise_elem = tree.xpath(sunrise_xpath)
    sunrise = sunrise_elem[0].text.strip() if sunrise_elem else "N/A"

    sunset_xpath = '//*[@id="wr-forecast"]/div[4]/div/div[1]/div[4]/div/div[1]/div[1]/span[2]/span[2]'
    sunset_elem = tree.xpath(sunset_xpath)
    sunset = sunset_elem[0].text.strip() if sunset_elem else "N/A"

    tide_times = scrape_tide_times()

    time_of_search = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return {
        'Time of Search': time_of_search,
        'High Temperature': high_temperature,
        'Low Temperature': low_temperature,
        'Current Temperature': current_temperature,
        'Weather Condition': weather_condition,
        'Wind Speed': wind_speed,
        'Humidity': humidity,
        'Pressure': pressure,
        'Visibility': visibility,
        'Location': location,
        'Wind Direction': wind_direction,
        'UV Index': uv_index,
        'Pollen': pollen_level,
        'Pollution': pollution_level,
        'Chance of Precipitation': chance_of_precipitation,
        'Sunset': sunset,
        'Sunrise': sunrise,
        'Low Tide Morning': tide_times[0][0],
        'High Tide Morning': tide_times[0][1],
        'Low Tide Evening': tide_times[1][0],
        'High Tide Evening': tide_times[1][1],
    }

# Function to save weather data to MySQL database
def save_to_mysql(weather_data):
    try:
        connection = mysql.connector.connect(
            host='f80b6byii2vwv8cx.chr7pe7iynqr.eu-west-1.rds.amazonaws.com',
            user='nlmwsz8at3gctlir',
            password='hencccpjtt6h7f2m',
            database='mb4rea5chn614zj5'
        )

        cursor = connection.cursor()

        sql = "INSERT INTO weather_data (time_of_search, high_temperature, low_temperature, current_temperature, weather_condition, wind_speed, humidity, pressure, visibility, location, wind_direction, uv_index, pollen_level, pollution_level, chance_of_precipitation, sunrise, sunset, low_tide_morning, high_tide_morning, low_tide_evening, high_tide_evening) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        values = (
            weather_data['Time of Search'],
            weather_data['High Temperature'],
            weather_data['Low Temperature'],
            weather_data['Current Temperature'],
            weather_data['Weather Condition'],
            weather_data['Wind Speed'],
            weather_data['Humidity'],
            weather_data['Pressure'],
            weather_data['Visibility'],
            weather_data['Location'],
            weather_data['Wind Direction'],
            weather_data['UV Index'],
            weather_data['Pollen'],
            weather_data['Pollution'],
            weather_data['Chance of Precipitation'],
            weather_data['Sunrise'],
            weather_data['Sunset'],
            weather_data['Low Tide Morning'],
            weather_data['High Tide Morning'],
            weather_data['Low Tide Evening'],
            weather_data['High Tide Evening']
        )

        cursor.execute(sql, values)

        connection.commit()
        print("Weather data inserted successfully into MySQL database")

    except Exception as e:
        print(f"Error inserting data into MySQL database: {str(e)}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def main():
    while True:
        weather_data = get_weather_data()

        # Print the extracted weather data (optional)
        for key, value in weather_data.items():
            print(f"{key}: {value}")

        print("\n")

        # Save weather data to MySQL database
        save_to_mysql(weather_data)

        # Wait for 30 minutes before fetching data again (optional)
        time.sleep(1800)

if __name__ == "__main__":
    main()
