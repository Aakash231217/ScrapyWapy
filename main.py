import json
import gzip
import threading
import logging
from queue import Queue
from selenium import webdriver
from time import sleep
from selenium.common.exceptions import NoSuchElementException

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class RestaurantScraper:
    def __init__(self, url, max_pages):
        self.url = url
        self.max_pages = max_pages
        self.queue = Queue()
        self.data_lock = threading.Lock()
        self.data = []

    def setup_driver(self):
        logging.info("Setting up the web driver.")
        self.driver = webdriver.Chrome()
        self.driver.get(self.url)
        self.driver.maximize_window()

    def close_driver(self):
        logging.info("Closing the web driver.")
        self.driver.quit()

    def scrape_page(self):
        for page in range(self.max_pages):
            logging.info(f"Scraping page {page + 1}")
            try:
                data = self.driver.find_element("id", "__NEXT_DATA__").get_attribute("innerHTML")
                parsed_data = json.loads(data)
                restaurant_list = parsed_data["props"]["initialReduxState"]["pageRestaurantsV2"]["entities"]["restaurantList"]
                for key in restaurant_list:
                    restaurant = self.extract_restaurant_info(restaurant_list[key])
                    with self.data_lock:
                        self.data.append(restaurant)
                        logging.info(f"Added restaurant: {restaurant['name']}")
                
                try:
                    load_more = self.driver.find_element(
                        "xpath", '//*[@id="page-content"]/div[4]/div/div/div[4]/div/button'
                    )
                    load_more.click()
                    sleep(10)
                except NoSuchElementException:
                    logging.info("No more pages to load.")
                    break
            except Exception as e:
                logging.error(f"Error scraping page {page + 1}: {e}")
                break

    def extract_restaurant_info(self, restaurant_data):
        # Extract restaurant information with error handling for missing fields
        def safe_get(dictionary, key, default="N/A"):
            return dictionary[key] if isinstance(dictionary, dict) and key in dictionary else default

        restaurant = {
            "id": safe_get(restaurant_data, "id"),
            "name": safe_get(restaurant_data, "name"),
            "cuisine": safe_get(restaurant_data, "cuisine", "N/A"),
            "rating": safe_get(safe_get(restaurant_data, "rating"), "averageRating", "N/A"),
            "estimate_time_of_delivery": safe_get(safe_get(restaurant_data, "etaRange"), "text", "N/A"),
            "distance_from_delivery_location": safe_get(restaurant_data, "distance", "N/A"),
            "promotional_offers": safe_get(restaurant_data, "promotions", "N/A"),
            "restaurant_notice": safe_get(restaurant_data, "restaurantNotice", "N/A"),
            "image_link": safe_get(restaurant_data, "imageUrl", "N/A"),
            "is_promo_available": safe_get(restaurant_data, "isPromoAvailable", False),
            "latitude": safe_get(restaurant_data, "latitude", "N/A"),
            "longitude": safe_get(restaurant_data, "longitude", "N/A"),
            "estimate_delivery_fee": safe_get(safe_get(restaurant_data, "deliveryFee"), "text", "N/A")
        }

        # Log extracted data for debugging
        logging.debug(f"Extracted data: {restaurant}")
        
        return restaurant

    def save_to_gzip_ndjson(self, filename):
        logging.info(f"Saving data to {filename}")
        with gzip.open(filename, 'wt', encoding='utf-8') as f:
            for restaurant in self.data:
                f.write(json.dumps(restaurant) + "\n")
        logging.info("Data saved successfully.")

    def worker(self):
        while not self.queue.empty():
            task = self.queue.get()
            try:
                task()
            finally:
                self.queue.task_done()

    def run(self):
        self.setup_driver()
        
        # Adding the scraping task to the queue
        for _ in range(1):
            self.queue.put(self.scrape_page)
        
        # Creating and starting threads
        threads = []
        for _ in range(4):  # Adjust the number of threads as needed
            t = threading.Thread(target=self.worker)
            t.start()
            threads.append(t)
        
        # Waiting for all threads to complete
        self.queue.join()
        for t in threads:
            t.join()
        
        self.close_driver()

        # Log the total number of restaurants scraped
        logging.info(f"Total number of restaurants scraped: {len(self.data)}")

        self.save_to_gzip_ndjson("restaurants.ndjson.gz")


if __name__ == "__main__":
    url = "https://food.grab.com/sg/en/restaurants"
    max_pages = 50
    scraper = RestaurantScraper(url, max_pages)
    scraper.run()
