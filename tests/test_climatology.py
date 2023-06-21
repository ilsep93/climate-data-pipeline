    def test_number_urls_constructed(self):
        """Expect 12 urls per available product"""

        temp = get_climatology("temp")
        available_products = len(temp.scenarios)
        urls = temp.url_constructor()

        assert len(urls) == available_products * 12
    
    def test_valid_urls_constructed(self):
        temp = get_climatology("temp")
        urls = temp.url_constructor()
        random_url = random.choice(urls)

        with rasterio.open(random_url, "r") as rast:
           raster = rast.read()
           assert isinstance(raster, np.ndarray)
