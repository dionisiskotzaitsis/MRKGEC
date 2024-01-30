.\bin\neo4j-admin.bat import --database=yelp.db --nodes=Day=import\day_header.csv,import\day.csv --nodes=Business=import\business_header.csv,import\business.csv --nodes=Category=import\category_header.csv,import\category.csv --nodes=User=import\user_header.csv,import\user.csv --nodes=City=import\city_header.csv,import\city.csv --nodes=Area=import\area_header.csv,import\area.csv --nodes=Month=import\month_header.csv,import\month.csv --nodes=Season=import\season_header.csv,import\season.csv --relationships=IN_CATEGORY=import\business_IN_CATEGORY_category_header.csv,import\business_IN_CATEGORY_category.csv --relationships=FRIENDS=import\user_FRIENDS_user_header.csv,import\user_FRIENDS_user.csv --relationships=REVIEW=import\user_REVIEW_business_header.csv,import\user_REVIEW_business.csv --relationships=IN_CITY=import\business_IN_CITY_city_header.csv,import\business_IN_CITY_city.csv --relationships=IN_AREA=import\business_IN_AREA_area_header.csv,import\business_IN_AREA_area.csv --relationships=IN=import\city_IN_area_header.csv,import\city_IN_area.csv --relationships=DAY_OF_MONTH=import\day_DAY_OF_MONTH_month_header.csv,import\day_DAY_OF_MONTH_month.csv --relationships=REVIEW_DAY=import\day_REVIEW_DAY_business_header.csv,import\day_REVIEW_DAY_business.csv --relationships=PART_OF=import\month_PART_OF_season_header.csv,import\month_PART_OF_season.csv --relationships=REVIEW_MONTH=import\month_REVIEW_MONTH_business_header.csv,import\month_REVIEW_MONTH_business.csv --relationships=REVIEW_SEASON=import\season_REVIEW_SEASON_business_header.csv,import\season_REVIEW_SEASON_business.csv --relationships=WROTE_IN_DAY=import\user_WROTE_IN_DAY_day_header.csv,import\user_WROTE_IN_DAY_day.csv --relationships=WROTE_IN_MONTH=import\user_WROTE_IN_MONTH_month_header.csv,import\user_WROTE_IN_MONTH_month.csv --multiline-fields=true --skip-bad-relationships=true
