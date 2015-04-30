from clay import config
from instagram.client import InstagramAPI

script_log = config.get_logger('monkey_job')


class job(object):

    TABLE_NAME = 'instagram_locations'

    def run(self, options=None):
        # self.create_run_flag()
        # start_time = datetime.utcnow()

        script_log.info("Running job: %s" % self.TABLE_NAME)

        data = self.get_mysql_data()
        self.update_marketo(data)

        # duration = int((datetime.utcnow() - start_time).total_seconds())
        # self.remove_run_flag()
        # self.log_success(duration)

    def test_insta(self, options=None):

        api = InstagramAPI(client_id=config.get("instagram.client_id"), client_secret=config.get("instagram.client_secret"))

        popular_media = api.media_popular(count=20)

        for media in popular_media:
            print(media.images['standard_resolution'].url)


if __name__ == "__main__":
    job = job()
    job.test_insta()
    # job.run()
