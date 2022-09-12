class Pubsub(dict):
    def __init__(self, connection_pool, shard_hint=None, ignore_subscribe_messages=False):
        # there is no connection pool, but we want a reference to the parent
        self.redis = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages

        self.channels = []
        self.patterns = []

    def publish(self, channel, message):
        """emulate publish"""
        if channel not in self:
            self.channels.append(channel)
            self[channel] = []

        self[channel].append(message)

    def reset(self):
        """emulate reset"""
        self.clear()
        self.channels = []

    def close(self):
        """emulate close"""
        self.reset()

    def encode(self, value):
        """emulate encode by calling the parent's"""
        return self.redis._encode(value)

    def on_connect(self, connection):
        """do nothing while mocking"""
        pass

    @property
    def subscribed(self):
        """emulate subscribed"""
        return bool(self.channels or self.patterns)

    def execute_command(self, *args, **kwargs):
        """do nothing while mocking"""
        return

    def parse_response(self, block=True, timeout=0):
        """do nothing while mocking"""
        return

    def psubscribe(self, *args, **kwargs):
        """call no callbacks while mocking"""
        return

    def punsubscribe(self, *args, **kwargs):
        """call no callbacks while mocking"""
        return

    def subscribe(self, *args, **kwargs):
        """call no callbacks while mocking"""
        return

    def unsubscribe(self, *args, **kwargs):
        """call no callbacks while mocking"""
        return

    def listen(self):
        """do nothing while mocking"""
        return

    def get_message(self, ignore_subscribe_messages=False, timeout=0):
        """do nothing while mocking"""
        return

    def handle_message(self, response, ignore_subscribe_messages=False):
        """do nothing while mocking"""
        return

    def run_in_thread(self, sleep_time=0):
        """do nothing while mocking"""
        return
